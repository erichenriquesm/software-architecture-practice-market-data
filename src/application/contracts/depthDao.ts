export interface DepthLevelView {
  readonly quantity: string;
  readonly price: string;
}

export interface DepthView {
  /** Highest price first. */
  readonly buys: readonly DepthLevelView[];
  /** Lowest price first. */
  readonly sells: readonly DepthLevelView[];
  readonly sequence: number;
  readonly asOf: string | null;
}

export interface DepthDao {
  /** The pre-aggregated levels of the last applied snapshot; an empty book (sequence 0) before the first one. */
  findByPrecision(marketId: string, precision: number): Promise<DepthView>;
}
