export interface TradeStatsView {
  readonly min: string | null;
  readonly max: string | null;
  readonly volume: string;
}

export interface TopOfBookView {
  readonly bestBid: string | null;
  readonly bestAsk: string | null;
  readonly updatedAt: Date;
}

/** Every range is [from, to). */
export interface MarketStatsDao {
  /** From the 1-minute candles: `from` and `to` must be whole minutes. */
  candleStats(marketId: string, from: Date, to: Date): Promise<TradeStatsView>;
  /** From the trades themselves: used for the partial minutes at the edges of a period. */
  tradeStats(marketId: string, from: Date, to: Date): Promise<TradeStatsView>;
  topOfBook(marketId: string): Promise<TopOfBookView | undefined>;
  lastTradeAt(marketId: string): Promise<Date | undefined>;
}
