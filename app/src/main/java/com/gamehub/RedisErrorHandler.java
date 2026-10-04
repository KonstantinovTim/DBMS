package com.gamehub;

import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import redis.clients.jedis.exceptions.JedisException;

@RestControllerAdvice
public class RedisErrorHandler {
    // Сюда попадёт и NOREPLICAS, и недоступность мастера во время failover
    @ExceptionHandler(JedisException.class)
    @ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
    public Map<String, String> handle(JedisException e) {
        return Map.of("error", String.valueOf(e.getMessage()));
    }
}