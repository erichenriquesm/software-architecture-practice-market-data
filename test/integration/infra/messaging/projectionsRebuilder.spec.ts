import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import type { MarketDataCore } from '../../../../src/infra/composition.js';
import { buildMarketDataCore } from '../../../../src/infra/composition.js';
import { projectTrade } from '../../../../src/infra/database/tradeProjector.js';
import { classifyMarketDataFailure } from '../../../../src/infra/messaging/consumers/consumedEvent.js';
import { ProjectionsRebuilder, ReplayNotReceivedError } from '../../../../src/infra/messaging/projectionsRebuilder.js';
import type { ReplayQueue } from '../../../../src/infra/messaging/replayQueue.js';
import { declareReplayQueue, replayHandler } from '../../../../src/infra/messaging/replayQueue.js';
import { loadEnvelopeValidator } from '../../../../src/infra/platform/messaging/envelope.js';
import type { TestDatabase } from '../../../support/database.js';
import { createTestDatabase } from '../../../support/database.js';
import { matchingEnvelope, snapshotFixture, tradeFixture } from '../../../support/fixtures.js';
import type { TestBroker } from '../../../support/rabbitmq.js';
import { createTestBroker } from '../../../support/rabbitmq.js';

let database: TestDatabase;
let broker: TestBroker;
let core: MarketDataCore;

beforeAll(async () => {
  database = await createTestDatabase();
  broker = await createTestBroker();
  core = buildMarketDataCore({ db: database.db, clock: { now: () => new Date('2026-10-28T12:00:00.000Z') } });
});

afterAll(async () => {
  await broker.close();
  await database.destroy();
});

const publishConfirmed = async (routingKey: string, body: object): Promise<void> => {
  const channel = await broker.connection.createConfirmChannel();
  channel.publish(broker.exchange, routingKey, Buffer.from(JSON.stringify(body)));
  await channel.waitForConfirms();
  await channel.close();
};

const newReplayQueue = async (): Promise<ReplayQueue> => {
  const replay = { queue: broker.uniqueName('marketData.replay'), exchange: broker.exchange };
  await declareReplayQueue(broker.channel, replay);
  return replay;
};

const rebuilderOf = async (replay: ReplayQueue, timeoutMs = 5_000) =>
  new ProjectionsRebuilder(database.db, await broker.connection.createChannel(), {
    queue: replay.queue,
    handler: replayHandler(core.bookProjector),
    validate: loadEnvelopeValidator(),
    classify: classifyMarketDataFailure,
    timeoutMs,
  });

describe('Projection rebuild from the matching republish', () => {
  test('Should empty the projections, apply the replay and stop at the replayCompleted marker', async () => {
    // Given: a stale projection, then what matching republishes (a trade twice, two snapshots, a broken message)
    const replay = await newReplayQueue();
    await database.db.transaction().execute((trx) => projectTrade(trx, tradeFixture({ quantity: '100', price: '1' })));
    const trade = tradeFixture({ quantity: '2', price: '84000', executedAt: '2026-10-28T11:00:00.000Z' });
    await publishConfirmed('matching.replay.tradeExecuted.BTC-USD', matchingEnvelope('matching.tradeExecuted', trade));
    await publishConfirmed('matching.replay.tradeExecuted.BTC-USD', matchingEnvelope('matching.tradeExecuted', trade));
    await publishConfirmed('matching.replay.bookSnapshot.BTC-USD', matchingEnvelope('matching.bookSnapshot', snapshotFixture({ sequence: 2, bids: [{ price: '83999', quantity: '3' }] })));
    await publishConfirmed('matching.replay.bookSnapshot.BTC-USD', matchingEnvelope('matching.bookSnapshot', snapshotFixture({ sequence: 1, bids: [{ price: '1', quantity: '1' }] })));
    await publishConfirmed('matching.replay.bookSnapshot.BTC-USD', { not: 'an envelope' });
    await publishConfirmed('matching.replay.completed', matchingEnvelope('matching.replayCompleted', { trades: 1, snapshots: 1 }));
    await publishConfirmed('matching.replay.tradeExecuted.BTC-USD', matchingEnvelope('matching.tradeExecuted', tradeFixture()));
    await publishConfirmed('matching.tradeExecuted.BTC-USD', matchingEnvelope('matching.tradeExecuted', tradeFixture()));

    // When
    const result = await (await rebuilderOf(replay)).rebuild();

    // Then: the regular key never reached the replay queue, and what came after the marker stays there
    expect(result).toEqual({ applied: 4, skipped: 1, published: { trades: 1, snapshots: 1 } });
    expect(await core.getMarket.execute({ marketId: 'BTC-USD' })).toMatchObject({ min: '84000.00000000', volume: '168000.00000000' });
    expect((await core.getDepth.execute('BTC-USD')).buys).toEqual([{ quantity: '3.00000000', price: '83999.00000000' }]);
    expect((await core.getTrades.execute({ marketId: 'BTC-USD' })).map((row) => row.tradeId)).toEqual([trade.tradeId]);
    await expect.poll(async () => (await broker.channel.checkQueue(replay.queue)).messageCount).toBe(1);
  });

  test('Should fail with a clear error when matching republishes nothing', async () => {
    // Given
    const replay = await newReplayQueue();

    // When & Then
    await expect((await rebuilderOf(replay, 300)).rebuild()).rejects.toBeInstanceOf(ReplayNotReceivedError);
  });
});
