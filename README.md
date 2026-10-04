# GameHub — бэкенд игровой платформы на Redis

Домашнее задание №1 по курсу «Системы управления базами данных».

REST API сервис, который управляет профилями игроков, рейтингом турнира, достижениями и уведомлениями. Redis используется как единственное хранилище и как кеш. Сервис работает с отказоустойчивой конфигурацией Redis: мастер, две реплики и три Sentinel.

## Архитектура

```
                      ┌──────────────────────┐
   HTTP :8080         │   gamehub-app        │
 ───────────────────▶ │   Spring Boot + Jedis│
                      └──────────┬───────────┘
                                 │ 1. «кто сейчас мастер?»
                                 ▼
            ┌────────────┬────────────┬────────────┐
            │ sentinel-1 │ sentinel-2 │ sentinel-3 │   кворум 2
            └─────┬──────┴─────┬──────┴─────┬──────┘
                  │  мониторинг и failover  │
                  ▼                         ▼
            ┌──────────────┐  репликация  ┌─────────────────┐
 2. команды │ redis-master │ ───────────▶ │ redis-replica-1 │
 ─────────▶ │  RDB + AOF   │ ───────────▶ │ redis-replica-2 │
            └──────────────┘              └─────────────────┘
```

| Компонент | Контейнер | Порт на хосте | Назначение |
|---|---|---|---|
| Приложение | `gamehub-app` | 8080 | REST API, фоновый потребитель уведомлений |
| Redis Master | `redis-master` | 6379 | Принимает запись. RDB + AOF, `volatile-lru`, защита от split-brain |
| Redis Replica 1 | `redis-replica-1` | 6380 | Копия мастера, только чтение |
| Redis Replica 2 | `redis-replica-2` | 6381 | Копия мастера, только чтение |
| Sentinel 1–3 | `sentinel-1..3` | 26379, 26380, 26381 | Обнаружение отказа мастера и автоматическое переключение |
| Redis Insight | `redis-insight` | 5540 | Визуальный мониторинг (опционально) |

Приложение не знает адрес мастера. Оно подключается к трём Sentinel, спрашивает у них текущий адрес мастера `mymaster` и подписывается на событие смены мастера. После failover приложение продолжает работать без перезапуска.

## Запуск

Требуется Docker с Docker Compose.

```bash
docker compose up -d --build
docker compose ps
```

Первая сборка занимает несколько минут: Maven скачивает зависимости. Должно быть запущено 8 контейнеров.

Проверка репликации и Sentinel:

```bash
docker compose exec redis-master redis-cli INFO replication
docker compose exec sentinel-1 redis-cli -p 26379 SENTINEL get-master-addr-by-name mymaster
```

Ожидается `connected_slaves:2` и адрес `redis-master 6379`.

Остановка:

```bash
docker compose down        # данные сохраняются в томах
docker compose down -v     # вместе с данными
```

Redis Insight: http://localhost:5540 → «Add Redis Database» → Host `redis-master`, Port `6379`.

## Структура данных

| Данные | Ключ | Тип Redis | Содержимое | TTL |
|---|---|---|---|---|
| Профиль игрока | `player:{id}` | Hash | `name`, `level`, `region`, `created_at` | нет |
| Кеш профиля | `cache:player:{id}` | String | JSON с данными профиля | 60 секунд |
| Счётчик входов | `logins:{id}` | String | число входов за сутки | 24 часа |
| Лидерборд | `tournament:main` | Sorted Set | member = id игрока, score = очки | нет |
| Достижения | `achievements:{id}` | Set | названия достижений | нет |
| Уведомления | `notifications` | Stream | `player_id`, `type`, `message`, `timestamp` | 7 дней |

## API

Базовый адрес: `http://localhost:8080`.

| Метод | Путь | Что делает | Команды Redis |
|---|---|---|---|
| POST | `/api/players/{id}` | Создать или обновить профиль | `HSET`, `HSETNX created_at`, `DEL cache:player:{id}` |
| GET | `/api/players/{id}` | Получить профиль (с кешем) | `GET cache:player:{id}`; при промахе `HGETALL` + `SETEX ... 60` |
| PATCH | `/api/players/{id}/level` | Изменить уровень | `HINCRBY`, `DEL cache:player:{id}`, `XADD notifications` |
| POST | `/api/players/{id}/login` | Зафиксировать вход | Lua-скрипт: `INCR` + `EXPIRE 86400` |
| POST | `/api/players/batch` | Массовое создание профилей | пачка `HSET` через pipeline |
| POST | `/api/leaderboard/score` | Начислить очки | `ZINCRBY tournament:main` |
| GET | `/api/leaderboard/top?limit=10` | Топ игроков | `ZREVRANGE ... WITHSCORES` |
| GET | `/api/leaderboard/rank/{playerId}` | Место игрока | `ZREVRANK`, `ZRANK` |
| POST | `/api/players/{id}/achievements` | Добавить достижение | `SADD` |
| GET | `/api/players/{id}/achievements/{name}` | Проверить наличие достижения | `SISMEMBER` |
| GET | `/api/players/{id1}/achievements/common/{id2}` | Общие достижения двух игроков | `SINTER` |

### Примеры

```bash
# Профиль
curl -X POST localhost:8080/api/players/1001 \
  -H 'Content-Type: application/json' \
  -d '{"name":"Pavel","level":42,"region":"EU"}'

curl -i localhost:8080/api/players/1001        # заголовок X-Cache: MISS или HIT

curl -X PATCH localhost:8080/api/players/1001/level \
  -H 'Content-Type: application/json' -d '{"delta":1}'

curl -X POST localhost:8080/api/players/1001/login

# Лидерборд
curl -X POST localhost:8080/api/leaderboard/score \
  -H 'Content-Type: application/json' -d '{"playerId":1001,"score":1500}'

curl 'localhost:8080/api/leaderboard/top?limit=10'
curl localhost:8080/api/leaderboard/rank/1001

# Достижения
curl -X POST localhost:8080/api/players/1001/achievements \
  -H 'Content-Type: application/json' -d '{"name":"first_win"}'

curl localhost:8080/api/players/1001/achievements/first_win
curl localhost:8080/api/players/1001/achievements/common/1002

# Массовая загрузка
curl -X POST 'localhost:8080/api/players/batch?pipeline=true' \
  -H 'Content-Type: application/json' \
  -d '[{"id":2001,"name":"Player2001","level":1,"region":"EU"},
       {"id":2002,"name":"Player2002","level":1,"region":"US"}]'
```

В ответе `rank` поле `place` — место от лидера (лидер = 1, вычисляется через `ZREVRANK`), поле `zrank` — позиция по возрастанию очков (`ZRANK`).

Если Redis недоступен или отклоняет запись, API возвращает `503` с текстом ошибки.

## Как это устроено

### Кеширование (Cache-Aside)

1. `GET /api/players/{id}` сначала читает `cache:player:{id}`.
2. При промахе профиль читается из хеша `player:{id}`, сериализуется в JSON и кладётся в кеш на 60 секунд.
3. При любом изменении профиля кеш удаляется (`DEL`), следующее чтение положит свежую версию.

Заголовок ответа `X-Cache` показывает, откуда пришли данные: `HIT` или `MISS`.

### Счётчик входов (Lua)

```lua
local count = redis.call('INCR', KEYS[1])
if count == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return count
```

`INCR` и `EXPIRE` выполняются атомарно: счётчик не может остаться без TTL, если приложение упадёт между двумя командами. TTL ставится только при первом входе, поэтому счётчик сбрасывается ровно через 24 часа после него.

### Уведомления (Streams)

- При изменении уровня в поток `notifications` добавляется сообщение (`XADD`).
- Параметр `MINID ~` при каждом добавлении удаляет сообщения старше 7 дней.
- Группа `notifications-group` создаётся при старте приложения: `XGROUP CREATE notifications notifications-group $ MKSTREAM`.
- Потребитель `consumer-1` работает в фоновом потоке приложения: читает через `XREADGROUP`, выводит сообщение в лог и подтверждает через `XACK`.

Посмотреть обработанные уведомления:

```bash
docker logs gamehub-app | grep Уведомление
```

### Конвейеризация (Pipeline)

`POST /api/players/batch` отправляет все команды одной пачкой и читает ответы разом. Параметр `?pipeline=false` отправляет те же команды по одной — для сравнения. Время выполнения возвращается в поле `elapsedMs`.

### Подключение к Redis

Класс `RedisClient` использует `JedisSentinelPool`:

- адрес мастера запрашивается у Sentinel, а не задаётся в коде;
- таймаут соединения и чтения — 2 секунды;
- при старте приложение до 30 раз пытается подключиться и выполнить `PING`;
- при обрыве связи команда повторяется до 3 раз;
- ошибки, которые вернул сам Redis (например, `NOREPLICAS`), не повторяются.

## Конфигурация

### `redis.conf`

Один файл для мастера и реплик: после failover любая реплика может стать мастером и должна вести себя так же. Реплики дополнительно получают флаги `--replicaof redis-master 6379 --replica-read-only yes`.

| Параметр | Значение | Зачем |
|---|---|---|
| `save 900 1`, `300 10`, `60 10000` | RDB | Снимок памяти по расписанию |
| `appendonly yes`, `appendfsync everysec` | AOF | Журнал команд, при сбое теряется не больше секунды |
| `auto-aof-rewrite-percentage 100` | AOF | Сжатие журнала, когда он вырос вдвое |
| `maxmemory 256mb` | память | Лимит памяти |
| `maxmemory-policy volatile-lru` | вытеснение | Вытесняются только ключи с TTL (кеш, счётчики); профили и лидерборд не затрагиваются |
| `min-replicas-to-write 1` | split-brain | Мастер принимает запись, только если на связи есть хотя бы одна реплика |
| `min-replicas-max-lag 10` | split-brain | Реплика считается живой, если отвечала не более 10 секунд назад |

### `sentinel.conf`

| Параметр | Зачем |
|---|---|
| `sentinel monitor mymaster redis-master 6379 2` | Следить за мастером; кворум 2 из 3 для признания отказа |
| `sentinel down-after-milliseconds mymaster 5000` | Через 5 секунд молчания мастер помечается недоступным |
| `sentinel failover-timeout mymaster 60000` | Таймаут процедуры переключения |
| `sentinel parallel-syncs mymaster 1` | Реплики пересинхронизируются с новым мастером по одной |
| `sentinel resolve-hostnames yes`, `announce-hostnames yes` | Работа с именами контейнеров вместо IP |

`sentinel1.conf`, `sentinel2.conf`, `sentinel3.conf` — копии `sentinel.conf`, по одной на каждый Sentinel.

## Проверка отказоустойчивости

### Автоматическое переключение мастера

```bash
docker stop redis-master
# подождать около 15 секунд
docker compose exec sentinel-1 redis-cli -p 26379 SENTINEL get-master-addr-by-name mymaster
curl -X POST localhost:8080/api/players/1001/login
```

Sentinel возвращает адрес одной из реплик, запрос к API выполняется без перезапуска приложения.

```bash
docker start redis-master
docker compose exec redis-master redis-cli INFO replication   # role:slave
```

Вернувшийся узел автоматически становится репликой нового мастера. Возврат к исходному состоянию: `docker compose down && docker compose up -d`.

### Защита от split-brain

```bash
docker pause redis-replica-1 redis-replica-2
# подождать около 10 секунд
docker compose exec redis-master redis-cli SET test value
# (error) NOREPLICAS Not enough good replicas to write.
docker unpause redis-replica-1 redis-replica-2
```

Мастер без живых реплик отклоняет запись, чтение продолжает работать.

## Самопроверка

```bash
curl -s localhost:8080/api/players/1001 > /dev/null && python3 dz1_check.py
```

Скрипт проверяет состояние Redis, поэтому перед запуском нужно выполнить запросы из раздела «Примеры» (профиль 1001, очки, достижение, вход, изменение уровня, массовая загрузка).

## Структура проекта

```
gamehub/
├── docker-compose.yml
├── redis.conf
├── sentinel.conf            # шаблон
├── sentinel1.conf           # конфиги трёх Sentinel
├── sentinel2.conf
├── sentinel3.conf
├── dz1_check.py             # скрипт самопроверки
└── app/
    ├── Dockerfile
    ├── pom.xml
    └── src/main/java/com/gamehub/
        ├── GamehubApplication.java     # точка входа
        ├── RedisClient.java            # подключение через Sentinel, ретраи
        ├── RedisErrorHandler.java      # ошибки Redis → HTTP 503
        ├── PlayerController.java       # профили, кеш, входы, достижения, batch
        ├── LeaderboardController.java  # лидерборд
        └── NotificationConsumer.java   # потребитель потока уведомлений
```

## Технологии

Java 21, Spring Boot 3.5, Jedis 5.2, Redis 7, Docker Compose.
