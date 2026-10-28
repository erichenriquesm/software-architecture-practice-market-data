import type { DepthDao, DepthView } from '../../src/application/contracts/depthDao.js';

export class FakeDepthDao implements DepthDao {
  readonly calls: { marketId: string; precision: number }[] = [];

  constructor(private readonly view: DepthView = { buys: [], sells: [], sequence: 0, asOf: null }) {}

  async findByPrecision(marketId: string, precision: number): Promise<DepthView> {
    this.calls.push({ marketId, precision });
    return this.view;
  }
}
