import Fastify from 'fastify';
import { buildMarketDataCore, serviceName } from './composition.js';
import { createDatabase, PostgresProbe } from './database/postgres.js';
import { registerHealthRoutes } from './http/health.js';
import { bookHandler, bookQueue } from './messaging/consumers/bookConsumer.js';
import { classifyMarketDataFailure } from './messaging/consumers/consumedEvent.js';
import { tradesHandler, tradesQueue } from './messaging/consumers/tradesConsumer.js';
import { declareReplayQueue, replayQueue } from './messaging/replayQueue.js';
import type { AmqpConnection } from './platform/messaging/connection.js';
import { connectRabbitMq, rabbitMqUrlFromEnv } from './platform/messaging/connection.js';
import type { ConsumerOptions } from './platform/messaging/consumer.js';
import { Consumer } from './platform/messaging/consumer.js';
import { loadEnvelopeValidator } from './platform/messaging/envelope.js';
import { declareConsumerQueue } from './platform/messaging/topology.js';
import { healthPortSetting, postgresSettings } from './settings.js';

const db = createDatabase(postgresSettings());
const core = buildMarketDataCore({ db });
const validate = loadEnvelopeValidator();
const log = Fastify({ logger: true }).log;

const consumerOptions: readonly Omit<ConsumerOptions, 'validate' | 'classify'>[] = [
  { definition: tradesQueue, handler: tradesHandler },
  { definition: bookQueue, handler: bookHandler(core.bookProjector), inbox: false },
];

const startConsumer = async (connection: AmqpConnection, options: Omit<ConsumerOptions, 'validate' | 'classify'>): Promise<Consumer> => {
  const channel = await connection.createChannel();
  for (const binding of options.definition.bindings) await channel.checkExchange(binding.exchange);
  await declareConsumerQueue(channel, options.definition);
  const consumer = new Consumer(core.runner, channel, { ...options, validate, classify: classifyMarketDataFailure });
  await consumer.start();
  return consumer;
};

const connection = await connectRabbitMq(rabbitMqUrlFromEnv());
let brokerUp = true;
const topology = await connection.createChannel();
await topology.checkExchange(replayQueue.exchange);
await declareReplayQueue(topology);
await topology.close();
const consumers = await Promise.all(consumerOptions.map((options) => startConsumer(connection, options)));

const healthServer = Fastify({ logger: false });
registerHealthRoutes(healthServer, serviceName, [new PostgresProbe(db), { dependency: 'rabbitmq', isHealthy: async () => brokerUp }]);
await healthServer.listen({ host: '0.0.0.0', port: healthPortSetting() });

let stopping = false;
const shutdown = async (): Promise<void> => {
  stopping = true;
  await Promise.all(consumers.map((consumer) => consumer.stop().catch(() => undefined)));
  await connection.close().catch(() => undefined);
  await healthServer.close();
  await db.destroy();
};

/** No in-process reconnect: on a lost broker connection the worker exits and the container restarts it (the inbox makes it lossless). */
const exitOnBrokerLoss = (reason: unknown): void => {
  if (stopping) return;
  brokerUp = false;
  log.error({ detail: reason instanceof Error ? reason.message : reason }, 'rabbitmq connection lost, exiting');
  // Exit at once: closing channels on a dead connection can hang, and the outbox + inbox make a hard exit safe.
  process.exit(1);
};
connection.on('error', (error: unknown) => exitOnBrokerLoss(error));
connection.on('close', () => exitOnBrokerLoss('closed'));

process.once('SIGTERM', shutdown);
process.once('SIGINT', shutdown);
