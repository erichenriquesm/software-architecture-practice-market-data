import type { BookProjector } from '../database/bookProjector.js';
import type { AmqpConnection } from '../platform/messaging/connection.js';
import type { MessageHandler } from '../platform/messaging/consumer.js';
import { bookHandler } from './consumers/bookConsumer.js';
import { tradesHandler } from './consumers/tradesConsumer.js';

type Channel = Awaited<ReturnType<AmqpConnection['createChannel']>>;

export interface ReplayQueue {
  readonly queue: string;
  readonly exchange: string;
}

/** Empty day to day: only receives what matching republishes (matching.replay.*) for a projection rebuild. */
export const replayQueue: ReplayQueue = { queue: 'marketData.replay', exchange: 'matching.events' };

export const replayCompletedType = 'matching.replayCompleted';

export const declareReplayQueue = async (channel: Channel, replay: ReplayQueue = replayQueue): Promise<void> => {
  await channel.assertQueue(replay.queue, { durable: true, arguments: { 'x-queue-type': 'quorum' } });
  await channel.bindQueue(replay.queue, replay.exchange, 'matching.replay.#');
};

/** Applies the republished events with the same projectors as the regular consumers. */
export const replayHandler = (bookProjector: BookProjector): MessageHandler => {
  const book = bookHandler(bookProjector);
  return async (envelope, trx, part) => {
    if (envelope.type === 'matching.tradeExecuted') return tradesHandler(envelope, trx, part);
    if (envelope.type === 'matching.bookSnapshot') return book(envelope, trx, part);
  };
};
