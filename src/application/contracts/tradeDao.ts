export interface TradeView {
  readonly tradeId: string;
  readonly buyOrderId: string;
  readonly sellOrderId: string;
  readonly side: string;
  readonly quantity: string;
  readonly price: string;
  readonly timestamp: string;
}

export interface TradeDao {
  /** Most recent first; `before` is exclusive. */
  listRecent(marketId: string, limit: number, before?: Date): Promise<TradeView[]>;
}
