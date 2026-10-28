import { Redis } from 'ioredis';
import { buildMarketDataApi, rateLimitPrefix } from './composition.js';
import { createDatabase } from './database/postgres.js';
import { RedisRateLimiter } from './platform/http/rateLimit/rateLimiter.js';
import { createTrustedProxy } from './platform/http/trustedProxy.js';
import { apiPortSetting, postgresSettings, rateLimitSetting, redisSettings, trustedProxyHostSetting } from './settings.js';

const db = createDatabase(postgresSettings());
const redis = new Redis({
  ...redisSettings(),
  enableOfflineQueue: false,
  commandTimeout: 100,
  maxRetriesPerRequest: 1,
}).on('error', () => undefined);

const edgeProxy = createTrustedProxy({
  hostname: trustedProxyHostSetting(),
  onError: (error) => app.log.warn({ err: error }, 'edge lookup failed, keeping the last known address'),
});

const { app } = buildMarketDataApi({
  db,
  rateLimiter: new RedisRateLimiter(redis, rateLimitPrefix),
  rateLimits: { publicReadPerIp: rateLimitSetting('RATE_LIMITS_PUBLIC_READ_PER_IP', '1200/1m') },
  trustProxy: edgeProxy.trust,
  logger: true,
});

const shutdown = async (): Promise<void> => {
  edgeProxy.stop();
  await app.close();
  redis.disconnect();
  await db.destroy();
};
process.once('SIGTERM', shutdown);
process.once('SIGINT', shutdown);

await edgeProxy.start();
await app.listen({ host: '0.0.0.0', port: apiPortSetting() });
