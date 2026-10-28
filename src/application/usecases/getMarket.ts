import { combineStats } from '../calculations/combineStats.js';
import type { PeriodSegment } from '../calculations/marketPeriod.js';
import { latestInstant, marketPeriod, splitByMinute } from '../calculations/marketPeriod.js';
import { spread } from '../calculations/spread.js';
import type { Clock } from '../contracts/clock.js';
import type { MarketCatalog } from '../contracts/marketCatalog.js';
import type { MarketStatsDao, TradeStatsView } from '../contracts/marketStatsDao.js';
import { knownMarket } from './knownMarket.js';

export interface GetMarketInput {
  readonly marketId: string;
  readonly startDate?: string | undefined;
  readonly endDate?: string | undefined;
}

export interface GetMarketOutput {
  readonly spread: string | null;
  readonly min: string | null;
  readonly max: string | null;
  readonly volume: string;
  readonly asOf: string | null;
}

export class GetMarket {
  constructor(
    private readonly marketStatsDao: MarketStatsDao,
    private readonly marketCatalog: MarketCatalog,
    private readonly clock: Clock,
  ) {}

  async execute(input: GetMarketInput): Promise<GetMarketOutput> {
    knownMarket(this.marketCatalog, input.marketId);
    const period = marketPeriod(input.startDate, input.endDate, this.clock.now());
    const parts = await Promise.all(splitByMinute(period).map((segment) => this.statsOf(input.marketId, segment)));
    const [topOfBook, lastTradeAt] = await Promise.all([
      this.marketStatsDao.topOfBook(input.marketId),
      this.marketStatsDao.lastTradeAt(input.marketId),
    ]);
    const output: GetMarketOutput = {
      spread: spread(topOfBook?.bestBid ?? null, topOfBook?.bestAsk ?? null),
      ...combineStats(parts),
      asOf: latestInstant(topOfBook?.updatedAt, lastTradeAt),
    };
    return output;
  }

  private statsOf(marketId: string, segment: PeriodSegment): Promise<TradeStatsView> {
    if (segment.source === 'candles') return this.marketStatsDao.candleStats(marketId, segment.from, segment.to);
    return this.marketStatsDao.tradeStats(marketId, segment.from, segment.to);
  }
}
