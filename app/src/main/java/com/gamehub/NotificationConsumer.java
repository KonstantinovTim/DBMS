package com.gamehub;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.StreamEntry;

@Component
public class NotificationConsumer {
    private static final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);

    private static final String STREAM = "notifications";
    private static final String GROUP = "notifications-group";
    private static final String CONSUMER = "consumer-1";

    private final RedisClient redis;

    public NotificationConsumer(RedisClient redis) {
        this.redis = redis;
    }

    // Запускаем фоновый поток, когда приложение полностью поднялось
    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        Thread thread = new Thread(this::run, "notification-consumer");
        thread.setDaemon(true);
        thread.start();
    }

    private void run() {
        ensureGroup();
        while (!Thread.currentThread().isInterrupted()) {
            try {
                // XREADGROUP GROUP notifications-group consumer-1 COUNT 10 BLOCK 1000 STREAMS notifications >
                XReadGroupParams params = XReadGroupParams.xReadGroupParams().count(10).block(1000);
                Map<String, StreamEntryID> streams = Map.of(STREAM, StreamEntryID.XREADGROUP_UNDELIVERED_ENTRY);
                List<Map.Entry<String, List<StreamEntry>>> result =
                        redis.execute(jedis -> jedis.xreadGroup(GROUP, CONSUMER, params, streams));
                if (result == null) {
                    continue; // за секунду новых сообщений не пришло
                }
                for (Map.Entry<String, List<StreamEntry>> stream : result) {
                    for (StreamEntry message : stream.getValue()) {
                        log.info("Уведомление {}: {}", message.getID(), message.getFields()); // «обработка»
                        redis.execute(jedis -> jedis.xack(STREAM, GROUP, message.getID()));
                    }
                }
            } catch (JedisException e) {
                log.warn("Ошибка чтения потока: {}", e.getMessage());
                sleep(1000);
            }
        }
    }

    // XGROUP CREATE notifications notifications-group $ MKSTREAM
    private void ensureGroup() {
        while (true) {
            try {
                redis.execute(jedis -> jedis.xgroupCreate(STREAM, GROUP, StreamEntryID.XGROUP_LAST_ENTRY, true));
                log.info("Группа {} создана", GROUP);
                return;
            } catch (JedisDataException e) {
                if (e.getMessage() != null && e.getMessage().contains("BUSYGROUP")) {
                    return; // группа уже существует — это нормально
                }
                log.warn("Не удалось создать группу: {}", e.getMessage());
                sleep(1000);
            } catch (JedisException e) {
                log.warn("Redis недоступен: {}", e.getMessage());
                sleep(1000);
            }
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}