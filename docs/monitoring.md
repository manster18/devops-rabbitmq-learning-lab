# Теория: Метрики и алерты

> Дополняет раздел [«Настройка мониторинга»](../README.md#настройка-мониторинга) в корневом README.

## 1. Откуда берутся метрики

RabbitMQ отдаёт метрики нативно через встроенный плагин `rabbitmq_prometheus` на порту `15692` (`GET /metrics`). Плагин уже включён в образе `rabbitmq:4.1-management`, а [`rabbitmq.conf`](../rabbitmq.conf) добавляет:

```ini
prometheus.return_per_object_metrics = true
```

Без этой настройки `/metrics` отдаёт только агрегированные (cluster-wide) значения — без разбивки по конкретной очереди (`queue="orders"`). Наш дашборд строится именно на per-object метриках, поэтому этот флаг обязателен.

## 2. Ключевые метрики по уровням

Важно понимать, что не все метрики существуют на всех уровнях (queue / channel / node) — это частая ошибка при написании PromQL-запросов.

### Уровень очереди (label `queue`)

| Метрика | Смысл |
|---------|-------|
| `rabbitmq_queue_messages_ready` | Сообщения, готовые к доставке |
| `rabbitmq_queue_messages_unacked` | Доставлены consumer-у, но ещё не подтверждены |
| `rabbitmq_queue_messages` | Ready + unacked (общая глубина очереди) |
| `rabbitmq_queue_messages_published_total` | Счётчик опубликованных в очередь сообщений |
| `rabbitmq_queue_consumers` | Число подписанных consumer-ов |
| `rabbitmq_queue_consumer_utilisation` | Доля времени, когда consumer-ы были готовы принимать сообщения |

### Уровень канала (без label `queue`!)

| Метрика | Смысл |
|---------|-------|
| `rabbitmq_channel_messages_published_total` | Опубликовано на канале |
| `rabbitmq_channel_messages_delivered_total` | Доставлено consumer-ам (auto-ack режим) |
| `rabbitmq_channel_messages_acked_total` | Подтверждено consumer-ами (**не** `_acknowledged_total` — частая опечатка) |
| `rabbitmq_channel_messages_unacked` | Доставлено, но не подтверждено на канале |

> ⚠️ **Частая ошибка:** метрик `rabbitmq_queue_messages_delivered_total` и `rabbitmq_queue_messages_acknowledged_total` **не существует** — delivered/acked считаются только на уровне канала, без привязки к конкретной очереди.

### Уровень узла

| Метрика | Смысл |
|---------|-------|
| `rabbitmq_process_resident_memory_bytes` | Память, используемая процессом Erlang VM |
| `rabbitmq_resident_memory_limit_bytes` | Порог `vm_memory_high_watermark` в байтах |
| `rabbitmq_disk_space_available_bytes` | Свободное место на диске (нативная метрика — надёжнее, чем `node_exporter` с mountpoint контейнера) |
| `rabbitmq_connections` | Текущее число подключений |

## 3. Дашборд Grafana

Файл [`monitoring/grafana/provisioning/dashboards/rabbitmq.json`](../monitoring/grafana/provisioning/dashboards/rabbitmq.json) — это «сырая» модель дашборда Grafana (поле `panels` на верхнем уровне), а не обёртка формата API-импорта (`{"dashboard": ..., "overwrite": ...}`). Для file-provisioning нужен именно первый вариант — иначе Grafana не сможет прочитать файл.

Панели дашборда:

1. **Messages in Queue (orders)** — Ready / Unacked / Total.
2. **Publish Rate — orders** — `rate(rabbitmq_queue_messages_published_total{queue="orders"}[1m])`.
3. **Consumers** / **Connections** — текущее состояние.
4. **Memory Usage** / **Disk Space Available** — gauge-панели с порогами.
5. **DLQ Messages** — глубина `orders-dlq`.
6. **Channel Operations** — publish/deliver/ack rate в целом по кластеру (не по конкретной очереди — см. предупреждение выше).

## 4. Алертинг

Правила лежат в [`monitoring/alerts.yml`](../monitoring/alerts.yml) и подключены в `prometheus.yml` через `rule_files`. Посмотреть их состояние: `http://localhost:9090/alerts`.

| Алерт | Условие | Severity |
|-------|---------|----------|
| `RabbitMQQueueGrowing` | Очередь выросла на 1000+ сообщений за 5 минут | warning |
| `RabbitMQNoConsumers` | 0 consumer-ов на очереди дольше 2 минут | critical |
| `RabbitMQDiskLow` | Свободного места меньше 2GB | critical |
| `RabbitMQHighMemory` | Резидентная память > 80% от лимита watermark | warning |

## 5. Полезные PromQL-запросы для практики

```promql
# Скорость роста очереди orders за последние 5 минут
increase(rabbitmq_queue_messages{queue="orders"}[5m])

# Доля unacked от общей глубины очереди (насколько consumer-ы отстают)
rabbitmq_queue_messages_unacked{queue="orders"} / rabbitmq_queue_messages{queue="orders"}

# Память в процентах от лимита high watermark
rabbitmq_process_resident_memory_bytes / rabbitmq_resident_memory_limit_bytes * 100
```

## 6. О предупреждении `management_metrics_collection` в логах

При старте RabbitMQ 4.x в логах может появляться предупреждение:

```
[warning] Deprecated features: `management_metrics_collection`: Feature `management_metrics_collection` is deprecated.
```

Это **не ошибка**. `management_metrics_collection` — статистика, на которой держится Management UI (графики очередей, панель «Get messages»). RabbitMQ рекомендует переходить на Prometheus-плагин, но сама фича по состоянию на RabbitMQ 4.x остаётся `permitted_by_default` (полностью рабочей), версия удаления не определена (см. [official deprecation list](https://www.rabbitmq.com/release-information/deprecated-features-list); текущий статус на живом узле — `rabbitmq-diagnostics list_deprecated_features`).

Само предупреждение **нельзя заглушить, оставив фичу включённой** — RabbitMQ логирует его при каждом старте, пока deprecated-фича активно используется, независимо от значения `deprecated_features.permit.*`. Убрать его можно только явным `= false`, но это **отключит** статистику и сломает Management UI (графики, «Get messages») — то есть часть лабораторных кейсов. Поэтому в `rabbitmq.conf` явно зафиксировано текущее (и пока единственно рабочее для этой лабы) намерение:

```ini
deprecated_features.permit.management_metrics_collection = true
```

Технически это не меняет поведение (`true` и так значение по умолчанию) — строка служит документацией намерения и страховкой на случай, если в будущем релизе RabbitMQ сменит дефолт на `denied by default`.

## 7. Практика

- Откройте Grafana (`http://localhost:3000`, `admin`/`admin`) и пройдите [Кейс 1](../README.md#кейс-1-отправка-и-потребление-сообщений-базовый-поток), наблюдая изменения на дашборде в реальном времени.
- Смоделируйте нехватку памяти в [Кейсе 7](../README.md#кейс-7-memorydisk-alarms--блокировка-producer) и понаблюдайте за алертом `RabbitMQHighMemory`.
