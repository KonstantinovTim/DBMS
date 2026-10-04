package com.gamehub;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.Pipeline;

@RestController
@RequestMapping("/api/players")
public class PlayerController {
    private static final int CACHE_TTL_SECONDS = 60;
    private static final int LOGIN_TTL_SECONDS = 86400;                 // 24 часа
    private static final Duration STREAM_RETENTION = Duration.ofDays(7);

    // INCR и EXPIRE одной атомарной операцией.
    // TTL ставится только при первом входе — счётчик живёт ровно сутки с него.
    private static final String LOGIN_SCRIPT = """
            local count = redis.call('INCR', KEYS[1])
            if count == 1 then
                redis.call('EXPIRE', KEYS[1], ARGV[1])
            end
            return count
            """;

    private final RedisClient redis;
    private final ObjectMapper mapper;

    public PlayerController(RedisClient redis, ObjectMapper mapper) {
        this.redis = redis;
        this.mapper = mapper;
    }

    public record BatchPlayer(long id, String name, int level, String region) {}
    public record PlayerRequest(String name, int level, String region) {}
    public record AchievementRequest(String name) {}
    public record LevelRequest(long delta) {}

    private static String playerKey(long id) { return "player:" + id; }
    private static String cacheKey(long id) { return "cache:player:" + id; }

    // Профиль (Hash)

    @PostMapping("/{id}")
    public Map<String, String> upsert(@PathVariable long id, @RequestBody PlayerRequest body) {
        return redis.execute(jedis -> {
            jedis.hset(playerKey(id), Map.of(
                    "name", body.name(),
                    "level", String.valueOf(body.level()),
                    "region", body.region()));
            jedis.hsetnx(playerKey(id), "created_at", String.valueOf(Instant.now().getEpochSecond()));
            jedis.del(cacheKey(id)); // профиль изменился — кеш больше не актуален
            return jedis.hgetAll(playerKey(id));
        });
    }

    // Cache-Aside: сначала кеш, при промахе — хеш, и положить результат в кеш
    @GetMapping("/{id}")
    public ResponseEntity<String> get(@PathVariable long id) {
        String cached = redis.execute(jedis -> jedis.get(cacheKey(id)));
        if (cached != null) {
            return json(cached, "HIT");
        }

        Map<String, String> profile = redis.execute(jedis -> jedis.hgetAll(playerKey(id)));
        if (profile.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Игрок не найден");
        }

        String body;
        try {
            body = mapper.writeValueAsString(profile);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
        redis.execute(jedis -> jedis.setex(cacheKey(id), CACHE_TTL_SECONDS, body)); // SET ... EX 60
        return json(body, "MISS");
    }

    private static ResponseEntity<String> json(String body, String cacheStatus) {
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .header("X-Cache", cacheStatus)
                .body(body);
    }

    // Уровень + уведомление (Stream)

    @PatchMapping("/{id}/level")
    public Map<String, Object> changeLevel(@PathVariable long id, @RequestBody LevelRequest body) {
        return redis.execute(jedis -> {
            if (!jedis.exists(playerKey(id))) {
                throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Игрок не найден");
            }
            long level = jedis.hincrBy(playerKey(id), "level", body.delta());
            jedis.del(cacheKey(id));

            long now = Instant.now().toEpochMilli();
            String minId = String.valueOf(now - STREAM_RETENTION.toMillis());
            jedis.xadd("notifications",
                    XAddParams.xAddParams().minId(minId).approximateTrimming(),
                    Map.of(
                            "player_id", String.valueOf(id),
                            "type", "level_up",
                            "message", "Новый уровень: " + level,
                            "timestamp", String.valueOf(now)));

            return Map.of("playerId", id, "level", level);
        });
    }

    // Счётчик входов (String + Lua)

    @PostMapping("/{id}/login")
    public Map<String, Object> login(@PathVariable long id) {
        Object count = redis.execute(jedis ->
                jedis.eval(LOGIN_SCRIPT, List.of("logins:" + id), List.of(String.valueOf(LOGIN_TTL_SECONDS))));
        return Map.of("playerId", id, "loginsToday", count);
    }

    // Достижения (Set)

    @PostMapping("/{id}/achievements")
    public Map<String, Object> addAchievement(@PathVariable long id, @RequestBody AchievementRequest body) {
        long added = redis.execute(jedis -> jedis.sadd("achievements:" + id, body.name()));
        return Map.of("achievement", body.name(), "added", added == 1);
    }

    @GetMapping("/{id}/achievements/{name}")
    public Map<String, Boolean> hasAchievement(@PathVariable long id, @PathVariable String name) {
        boolean has = redis.execute(jedis -> jedis.sismember("achievements:" + id, name));
        return Map.of("has", has);
    }

    @GetMapping("/{id1}/achievements/common/{id2}")
    public Set<String> commonAchievements(@PathVariable long id1, @PathVariable long id2) {
        return redis.execute(jedis -> jedis.sinter("achievements:" + id1, "achievements:" + id2));
    }

    // Массовая загрузка (Pipe)

    @PostMapping("/batch")
    public Map<String, Object> batch(@RequestBody List<BatchPlayer> players,
                                     @RequestParam(defaultValue = "true") boolean pipeline) {
        String createdAt = String.valueOf(Instant.now().getEpochSecond());
        long start = System.nanoTime();

        redis.execute(jedis -> {
            if (pipeline) {
                // Команды копятся в буфере и уходят одной пачкой на sync()
                Pipeline p = jedis.pipelined();
                for (BatchPlayer pl : players) {
                    p.hset(playerKey(pl.id()), fields(pl));
                    p.hsetnx(playerKey(pl.id()), "created_at", createdAt);
                    p.del(cacheKey(pl.id()));
                }
                p.sync();
            } else {
                // Для сравнения: каждая команда ждёт свой ответ
                for (BatchPlayer pl : players) {
                    jedis.hset(playerKey(pl.id()), fields(pl));
                    jedis.hsetnx(playerKey(pl.id()), "created_at", createdAt);
                    jedis.del(cacheKey(pl.id()));
                }
            }
            return null;
        });

        double elapsedMs = (System.nanoTime() - start) / 1_000_000.0;
        return Map.of("created", players.size(), "pipeline", pipeline, "elapsedMs", elapsedMs);
    }

    private static Map<String, String> fields(BatchPlayer pl) {
        return Map.of("name", pl.name(), "level", String.valueOf(pl.level()), "region", pl.region());
    }
}