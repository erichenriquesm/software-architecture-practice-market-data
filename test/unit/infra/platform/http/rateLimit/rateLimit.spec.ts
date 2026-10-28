import Fastify from 'fastify';
import { describe, expect, test } from 'vitest';
import { registerRateLimit } from '../../../../../../src/infra/platform/http/rateLimit/rateLimitPlugin.js';
import type { RateLimitCheck, RateLimiter } from '../../../../../../src/infra/platform/http/rateLimit/rateLimiter.js';
import type { RateLimitDecision } from '../../../../../../src/infra/platform/http/rateLimit/slidingWindow.js';
import { decide, windowPosition } from '../../../../../../src/infra/platform/http/rateLimit/slidingWindow.js';

/** The same sliding window as RedisRateLimiter, counted in a Map: lets the plugin be tested without Redis. */
class CountingRateLimiter implements RateLimiter {
  private readonly counts = new Map<string, number>();

  async check(check: RateLimitCheck): Promise<RateLimitDecision> {
    const position = windowPosition(check.nowMs, check.policy.windowMs);
    const keyOf = (bucketStart: number) => `${check.policy.name}:${check.subject}:${bucketStart}`;
    const currentKey = keyOf(position.currentBucketStart);
    const decision = decide({
      previousCount: this.counts.get(keyOf(position.previousBucketStart)) ?? 0,
      currentCount: this.counts.get(currentKey) ?? 0,
      elapsedMs: position.elapsedMs,
      cost: check.cost,
      policy: check.policy,
    });
    if (decision.allowed) this.counts.set(currentKey, (this.counts.get(currentKey) ?? 0) + check.cost);
    return decision;
  }
}

const brokenLimiter: RateLimiter = { check: async () => Promise.reject(new Error('redis down')) };

const policy = { name: 'orders', limit: 10, windowMs: 10_000 };

describe('Sliding window counter', () => {
  test('Should weight the previous window by the time left in the current one', () => {
    // previous=10 at 50% elapsed counts as 5; with current=4 the estimate is 9
    expect(decide({ previousCount: 10, currentCount: 4, elapsedMs: 5_000, cost: 1, policy })).toMatchObject({
      allowed: true,
      remaining: 0,
    });
    expect(decide({ previousCount: 10, currentCount: 5, elapsedMs: 5_000, cost: 1, policy })).toMatchObject({
      allowed: false,
      retryAfterSeconds: 5,
    });
  });

  test('Should forget the previous window entirely at the end of the current one', () => {
    expect(decide({ previousCount: 100, currentCount: 0, elapsedMs: 10_000, cost: 1, policy }).allowed).toBe(true);
  });

  test('Should count a heavier request with its weight', () => {
    expect(decide({ previousCount: 0, currentCount: 8, elapsedMs: 0, cost: 3, policy }).allowed).toBe(false);
  });
});

describe('Rate limit plugin', () => {
  const buildApp = (limiter: RateLimiter = new CountingRateLimiter()) => {
    const app = Fastify();
    registerRateLimit(app, {
      limiter,
      now: () => 1_000,
      groups: [
        {
          name: 'orders',
          matches: (request) => request.url === '/place_order',
          perIp: { limit: 100, windowMs: 10_000 },
          perSubject: {
            label: 'account',
            limit: 2,
            windowMs: 10_000,
            subjectOf: (request) => (request.body as { accountId?: string } | undefined)?.accountId,
          },
        },
      ],
    });
    app.post('/place_order', async () => ({ ok: true }));
    app.post('/other', async () => ({ ok: true }));
    return app;
  };

  test('Should limit per account and answer 429 with Retry-After and RateLimit headers', async () => {
    // Given
    const app = buildApp();
    const placeOrder = () => app.inject({ method: 'POST', url: '/place_order', payload: { accountId: 'a-1' } });

    // When
    const responses = [await placeOrder(), await placeOrder(), await placeOrder()];

    // Then
    expect(responses.map((response) => response.statusCode)).toEqual([200, 200, 429]);
    expect(responses[2]?.headers['retry-after']).toBe('9');
    expect(responses[0]?.headers['ratelimit-policy']).toBe('"ordersIp";q=100;w=10, "ordersAccount";q=2;w=10');
    expect(responses[0]?.headers['ratelimit']).toBe('"ordersAccount";r=1;t=9');
  });

  test('Should not limit a route outside every group', async () => {
    // Given
    const app = buildApp();

    // When
    const response = await app.inject({ method: 'POST', url: '/other' });

    // Then
    expect(response.statusCode).toBe(200);
    expect(response.headers['ratelimit']).toBeUndefined();
  });

  test('Should let the request through without RateLimit headers when the limiter fails (fail-open)', async () => {
    // Given
    const app = buildApp(brokenLimiter);

    // When
    const responses = await Promise.all([1, 2, 3].map(() => app.inject({ method: 'POST', url: '/place_order', payload: { accountId: 'a-1' } })));

    // Then
    expect(responses.map((response) => response.statusCode)).toEqual([200, 200, 200]);
    expect(responses[0]?.headers['ratelimit']).toBeUndefined();
  });
});
