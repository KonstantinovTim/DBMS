package com.gamehub;

import java.util.Set;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;

@Component
public class RedisClient {
    private static final Logger log = LoggerFactory.getLogger(RedisClient.class);

    private static final String MASTER_NAME = "mymaster";
    private static final Set<String> SENTINELS =
            Set.of("sentinel-1:26379", "sentinel-2:26379", "sentinel-3:26379");
    private static final int TIMEOUT_MS = 2000;
    private static final int MAX_ATTEMPTS = 3;

    private final JedisSentinelPool pool;

    public RedisClient() {
        this.pool = connect();
    }

    // Проверка соединения при старте
    private JedisSentinelPool connect() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setTestOnBorrow(true);

        for (int attempt = 1; attempt <= 30; attempt++) {
            try {
                JedisSentinelPool p = new JedisSentinelPool(MASTER_NAME, SENTINELS, config, TIMEOUT_MS);
                try (Jedis jedis = p.getResource()) {
                    jedis.ping();
                }
                log.info("Redis доступен, мастер: {}", p.getCurrentHostMaster());
                return p;
            } catch (JedisException e) {
                log.warn("Redis недоступен (попытка {}): {}", attempt, e.getMessage());
                sleep(1000);
            }
        }
        throw new IllegalStateException("Не удалось подключиться к Redis через Sentinel");
    }

    public <T> T execute(Function<Jedis, T> action) {
        JedisException last = null;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try (Jedis jedis = pool.getResource()) {
                return action.apply(jedis);
            } catch (JedisDataException e) {
                throw e; // ответ-ошибка от Redis
            } catch (JedisException e) {
                last = e; // проблема со связью
                sleep(300L * attempt);
            }
        }
        throw last;
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}