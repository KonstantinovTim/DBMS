package com.gamehub;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.web.bind.annotation.*;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import redis.clients.jedis.resps.Tuple;

@RestController
@RequestMapping("/api/leaderboard")
public class LeaderboardController {
    private static final String KEY = "tournament:main";
    private final RedisClient redis;

    public LeaderboardController(RedisClient redis) {
        this.redis = redis;
    }

    public record ScoreRequest(long playerId, double score) {}

    @PostMapping("/score")
    public Map<String, Object> addScore(@RequestBody ScoreRequest body) {
        double total = redis.execute(jedis ->
                jedis.zincrby(KEY, body.score(), String.valueOf(body.playerId())));
        return Map.of("playerId", body.playerId(), "score", total);
    }

    @GetMapping("/top")
    public List<Map<String, Object>> top(@RequestParam(defaultValue = "10") int limit) {
        List<Tuple> rows = redis.execute(jedis -> jedis.zrevrangeWithScores(KEY, 0, limit - 1));
        List<Map<String, Object>> result = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++) {
            Tuple t = rows.get(i);
            result.add(Map.of("place", i + 1, "playerId", t.getElement(), "score", t.getScore()));
        }
        return result;
    }

    @GetMapping("/rank/{playerId}")
    public Map<String, Object> rank(@PathVariable long playerId) {
        String member = String.valueOf(playerId);
        // ZREVRANK — позиция от лидера (с нуля), ZRANK — от наименьшего счёта
        Long revRank = redis.execute(jedis -> jedis.zrevrank(KEY, member));
        if (revRank == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Игрока нет в лидерборде");
        }
        Long rank = redis.execute(jedis -> jedis.zrank(KEY, member));
        return Map.of("playerId", playerId, "place", revRank + 1, "zrank", rank);
    }
}