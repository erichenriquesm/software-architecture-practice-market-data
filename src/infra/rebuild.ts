import { buildMarketDataCore } from './composition.js';
import { createDatabase } from './database/postgres.js';
import { classifyMarketDataFailure } from './messaging/consumers/consumedEvent.js';
import { ProjectionsRebuilder } from './messaging/projectionsRebuilder.js';
import { declareReplayQueue, replayHandler, replayQueue } from './messaging/replayQueue.js';
import { connectRabbitMq, rabbitMqUrlFromEnv } from './platform/messaging/connection.js';
import { loadEnvelopeValidator } from './platform/messaging/envelope.js';
import { durationSetting, postgresSettings } from './settings.js';

/*
 * Projection rebuild from what matching republishes (make projections-rebuild in backend/ runs the three steps):
 *   npm run projections:rebuild-prepare   empty marketData.replay, so nothing from an interrupted rebuild is left
 *   npm run projections:rebuild           empty every *_view table and apply marketData.replay until replayCompleted
 */
const prepare = process.argv[2] === 'prepare';
const db = createDatabase(postgresSettings());
const connection = await connectRabbitMq(rabbitMqUrlFromEnv());
try {
  const channel = await connection.createChannel();
  await channel.checkExchange(replayQueue.exchange);
  await declareReplayQueue(channel);
  if (prepare) {
    const { messageCount } = await channel.purgeQueue(replayQueue.queue);
    console.log(`${replayQueue.queue} emptied (${messageCount} left over messages)`);
  } else {
    const rebuilder = new ProjectionsRebuilder(db, channel, {
      queue: replayQueue.queue,
      handler: replayHandler(buildMarketDataCore({ db }).bookProjector),
      validate: loadEnvelopeValidator(),
      classify: classifyMarketDataFailure,
      timeoutMs: durationSetting('REBUILD_TIMEOUT', '60s'),
    });
    const result = await rebuilder.rebuild();
    console.log(
      `projections rebuilt: ${result.applied} events applied, ${result.skipped} skipped ` +
        `(matching republished ${result.published.trades} trades and ${result.published.snapshots} snapshots)`,
    );
  }
} catch (error) {
  console.error(error);
  process.exitCode = 1;
} finally {
  await connection.close();
  await db.destroy();
}
