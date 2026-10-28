import type { Kysely } from 'kysely';
import type { Database } from '../database/database.js';
import { truncateProjections } from '../database/projections.js';
import type { AmqpConnection } from '../platform/messaging/connection.js';
import type { FailureKind, MessageHandler } from '../platform/messaging/consumer.js';
import type { Envelope, EnvelopeValidator } from '../platform/messaging/envelope.js';
import { loadSchemaValidator } from '../platform/messaging/schemas.js';
import { replayCompletedType } from './replayQueue.js';

type Channel = Awaited<ReturnType<AmqpConnection['createChannel']>>;
type Message = NonNullable<Parameters<Parameters<Channel['consume']>[1]>[0]>;

export interface RebuildOptions {
  readonly queue: string;
  readonly handler: MessageHandler;
  readonly validate: EnvelopeValidator;
  readonly classify: (error: unknown) => FailureKind;
  /** Fails when nothing arrives for this long: the republish in matching did not run. */
  readonly timeoutMs: number;
  readonly prefetch?: number;
}

export interface RebuildResult {
  readonly applied: number;
  readonly skipped: number;
  readonly published: { readonly trades: number; readonly snapshots: number };
}

export class ReplayNotReceivedError extends Error {
  constructor(queue: string, timeoutMs: number) {
    super(`Nothing arrived on ${queue} for ${timeoutMs} ms: run the republish in matching (make projections-rebuild in backend/)`);
    this.name = 'ReplayNotReceivedError';
  }
}

/**
 * Rebuilds every projection: empties the *_view tables and applies what matching republished, with the same
 * projectors and without inbox (idempotent by tradeId, last-write-wins by sequence), until the replayCompleted marker.
 */
export class ProjectionsRebuilder {
  private readonly validateCompleted = loadSchemaValidator<RebuildResult['published']>(`src/infra/contracts/consumed/events/${replayCompletedType}.v1.schema.json`);

  constructor(
    private readonly db: Kysely<Database>,
    private readonly channel: Channel,
    private readonly options: RebuildOptions,
  ) {}

  async rebuild(): Promise<RebuildResult> {
    await truncateProjections(this.db);
    return this.consumeUntilCompleted();
  }

  private async apply(envelope: Envelope): Promise<'applied' | 'skipped'> {
    try {
      await this.db.transaction().execute((trx) => this.options.handler(envelope, trx, null));
      return 'applied';
    } catch (error) {
      if (this.options.classify(error) === 'transient') throw error;
      return 'skipped';
    }
  }

  private parse(message: Message): Envelope | undefined {
    try {
      return this.options.validate(JSON.parse(message.content.toString()));
    } catch {
      return undefined;
    }
  }

  private async consumeUntilCompleted(): Promise<RebuildResult> {
    await this.channel.prefetch(this.options.prefetch ?? 100);
    const counts = { applied: 0, skipped: 0 };
    let chain = Promise.resolve();
    let consumerTag: string | undefined;
    let timer: NodeJS.Timeout | undefined;
    let completed = false;

    return new Promise<RebuildResult>((resolve, reject) => {
      const fail = (error: unknown): void => {
        clearTimeout(timer);
        reject(error instanceof Error ? error : new Error(String(error)));
      };
      const armTimer = (): void => {
        clearTimeout(timer);
        timer = setTimeout(() => fail(new ReplayNotReceivedError(this.options.queue, this.options.timeoutMs)), this.options.timeoutMs);
      };
      const complete = async (envelope: Envelope): Promise<void> => {
        completed = true;
        clearTimeout(timer);
        if (consumerTag) await this.channel.cancel(consumerTag);
        resolve({ ...counts, published: this.validateCompleted(envelope.payload) });
      };
      const handle = async (message: Message): Promise<void> => {
        // Prefetched after the marker: belongs to a later rebuild, so it goes back to the queue.
        if (completed) return this.channel.nack(message, false, true);
        const envelope = this.parse(message);
        if (envelope?.type === replayCompletedType) {
          this.channel.ack(message);
          return complete(envelope);
        }
        counts[envelope ? await this.apply(envelope) : 'skipped'] += 1;
        this.channel.ack(message);
      };
      const onMessage = (message: Message): void => {
        if (!completed) armTimer();
        chain = chain.then(() => handle(message)).catch(fail);
      };
      this.channel.consume(this.options.queue, (message) => message && onMessage(message), { noAck: false }).then((consumed) => {
        consumerTag = consumed.consumerTag;
        armTimer();
      }, fail);
    });
  }
}
