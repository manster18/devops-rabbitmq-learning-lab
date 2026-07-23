# 🐇 RabbitMQ Learning Lab — Практическое руководство для DevOps-инженера

> **Актуально на июль 2026**
> Практическое руководство по изучению RabbitMQ с нуля до уровня DevOps-инженера. Лабораторная работа включает полный стек: брокер сообщений, микросервисы producer/consumer на Python, мониторинг через Prometheus и Grafana.

---

## Содержание

1. [Описание](#описание)
2. [Структура репозитория](#структура-репозитория)
3. [Технологический стек](#технологический-стек)
4. [Быстрый старт](#быстрый-старт)
5. [docker-compose.yml](#docker-composeyml)
6. [Микросервис 1: Producer](#микросервис-1-producer)
7. [Микросервис 2: Consumer](#микросервис-2-consumer)
8. [Настройка мониторинга](#настройка-мониторинга)
9. [Лабораторные кейсы](#лабораторные-кейсы)
10. [Кейсы для отладки](#кейсы-для-отладки)
11. [Бонусные кейсы](#бонусные-кейсы)
12. [Production: как надо и как не надо](#production-как-надо-и-как-не-надо)
13. [Чеклист для собеседования](#чеклист-для-собеседования)
14. [Дополнительные материалы](#дополнительные-материалы)
15. [Что было исправлено в этой версии](#что-было-исправлено-в-этой-версии)

---



## Описание

**RabbitMQ Learning Lab** — это полный практический курс для DevOps-инженеров, желающих освоить RabbitMQ на уровне, достаточном для работы в продакшене. Лабораторная работа включает:

- Запуск RabbitMQ через Docker Compose (+ отдельный compose-файл для 2-узлового кластера)
- Разработку и запуск микросервисов producer/consumer на Python (библиотека `pika`)
- Настройку мониторинга через Prometheus, Node Exporter и Grafana
- 8 лабораторных кейсов — от базовой отправки сообщений до кластерных сценариев
- 5 кейсов по отладке реальных проблем
- 3 бонусных кейса для продвинутого уровня
- Автоматическую проверку выполнения кейсов (`lab-solutions/case-*/verify.sh`)
- Теоретические материалы в `[docs/](docs/)`

**Целевая аудитория:** DevOps-инженеры с базовым пониманием Docker и TCP/IP.

---



## Структура репозитория

```
devops-rabbitmq-learning-lab/
├── README.md                          # Этот файл
├── .gitignore
├── .env.example                        # Шаблон переменных окружения (образы, порты, креды)
├── .env                                # Ваша локальная копия .env.example — git-ignored, создаётся вручную
├── docker-compose.yml                  # Оркестрация всех сервисов
├── docker-compose.cluster.yml          # Кейс 8: 2-узловой кластер
├── docker-compose.tls.yml              # Бонус 1: overlay (порт 5671 + mount ./tls)
├── rabbitmq.conf                       # Конфигурация брокера
├── definitions.json                    # Exchanges/queues/bindings, загружаемые при старте
│
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producer.py                     # Python-producer с приоритетами и exchanges
│
├── consumer/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── consumer.py                     # Consumer с retry и DLQ
│   └── dlq_handler.py                  # Обработчик мёртвых очередей (отдельный модуль)
│
├── monitoring/
│   ├── prometheus.yml                  # Конфигурация Prometheus
│   ├── alerts.yml                      # Правила алертинга
│   └── grafana/
│       ├── grafana.ini                 # Опциональные настройки Grafana
│       └── provisioning/
│           ├── datasources/
│           │   └── prometheus.yml      # Автоконфигурация datasource
│           └── dashboards/
│               ├── dashboard.yml       # Конфигурация провайдера дашбордов
│               └── rabbitmq.json       # Дашборд Grafana (file-provisioning формат)
│
├── scripts/
│   ├── setup.sh                        # Первичный запуск + ожидание healthy
│   ├── create-vhost.sh                 # Создание vhost + прав доступа
│   ├── load-test.py                    # Нагрузочное тестирование (concurrency, latency)
│   ├── tls-smoke-test.py               # Бонус 1: проверка mTLS publish/consume
│   ├── cleanup.sh                      # Очистка очередей / полный сброс стенда
│   └── generate-tls-certs.sh           # Генерация CA/server/client сертификатов (Бонус 1)
│
├── docs/
│   ├── concepts.md                     # Теория: exchanges, queues, bindings
│   ├── clustering.md                   # Теория: кластеризация и quorum queues
│   └── monitoring.md                   # Теория: метрики и алерты
│
└── lab-solutions/
    ├── _lib.sh                         # Общие функции для verify.sh
    ├── case-1/verify.sh
    ├── case-2/verify.sh
    ├── ...
    └── case-8/verify.sh
```

---



## Технологический стек


| Компонент          | Версия           | Назначение              |
| ------------------ | ---------------- | ----------------------- |
| **RabbitMQ**       | 4.1 (management) | Брокер сообщений        |
| **Docker Compose** | v2               | Оркестрация контейнеров |
| **Python**         | 3.12             | Язык микросервисов      |
| **pika**           | 1.3.2            | Python-клиент RabbitMQ  |
| **Prometheus**     | v2.54.1          | Сбор метрик             |
| **Grafana**        | 11.1.0           | Визуализация метрик     |
| **Node Exporter**  | v1.12.1          | Метрики системы         |




### Дополнительные зависимости (Python)

- `pika==1.3.2` — клиент для RabbitMQ
- `requests==2.32.3` — HTTP-запросы к Management API
- `prometheus-client==0.21.0` — метрики приложения

---



## Быстрый старт



### 1. Репозиторий

Репозиторий уже инициализирован локально. Если вы работаете с удалённой копией:

```bash
git clone <your-remote-url> devops-rabbitmq-learning-lab
cd devops-rabbitmq-learning-lab
```

Создайте локальный `.env` из шаблона (сам `.env` — в `.gitignore`, чтобы случайные локальные креды никогда не попали в git):

```bash
cp .env.example .env
```

Значения по умолчанию в `[.env.example](.env.example)` (учётные данные, версии образов, порты) уже готовы для локального обучения — менять их не обязательно. Если вы пропустите этот шаг и запустите `./scripts/setup.sh`, он создаст `.env` за вас автоматически.

### 2. Запустите все сервисы

```bash
docker compose up -d --build
# или воспользуйтесь готовым скриптом, который дождётся healthy RabbitMQ:
./scripts/setup.sh
```

> `producer` и `consumer` находятся в Compose profile `tools` и **не запускаются** автоматически — они предназначены для одноразовых запусков через `docker compose run`.



### 3. Проверьте здоровье сервисов

```bash
docker compose ps
```

`rabbitmq`, `prometheus` и `grafana` должны показать статус `healthy`/`running`.

### 4. Откройте веб-интерфейсы


| Сервис              | URL                                              | Логин / Пароль    |
| ------------------- | ------------------------------------------------ | ----------------- |
| RabbitMQ Management | [http://localhost:15672](http://localhost:15672) | `guest` / `guest` |
| Grafana             | [http://localhost:3000](http://localhost:3000)   | `admin` / `admin` |
| Prometheus          | [http://localhost:9090](http://localhost:9090)   | —                 |




### 5. Запустите первого producer

```bash
docker compose run --rm producer python producer.py --count 10
```



### 6. Запустите consumer в отдельном терминале

```bash
docker compose run --rm consumer python consumer.py
```



### 7. Очистка между кейсами

```bash
./scripts/cleanup.sh          # purge всех lab-очередей, стенд не останавливается
./scripts/cleanup.sh --full   # docker compose down -v — полный сброс
```

---



## docker-compose.yml

Полный файл лежит в `[docker-compose.yml](docker-compose.yml)`. Ключевые моменты:

- Все креды/версии/порты берутся из `.env` (создаётся из `[.env.example](.env.example)`) через `${VAR:-default}` — не хардкодятся.
- `node-exporter` — добавлен, так как `monitoring/prometheus.yml` скрейпит `node-exporter:9100`, а без самого сервиса эта job всегда была бы `DOWN`.
- `rabbitmq.conf` и `definitions.json` монтируются в контейнер `rabbitmq` как read-only volumes.
- `producer`/`consumer` — Compose profile `tools`, запускаются только через `docker compose run --rm <service> ...`.

```yaml
services:
  rabbitmq:
    image: rabbitmq:${RABBITMQ_IMAGE_TAG:-4.1-management}
    container_name: rabbitmq
    hostname: rabbitmq
    ports:
      - "${RABBITMQ_AMQP_PORT:-5672}:5672"
      - "${RABBITMQ_MANAGEMENT_PORT:-15672}:15672"
      - "${RABBITMQ_METRICS_PORT:-15692}:15692"
    environment:
      RABBITMQ_DEFAULT_USER: ${RABBITMQ_USER:-guest}
      RABBITMQ_DEFAULT_PASS: ${RABBITMQ_PASS:-guest}
      RABBITMQ_DEFAULT_VHOST: ${RABBITMQ_VHOST:-/}
    volumes:
      - rabbitmq_data:/var/lib/rabbitmq
      - ./rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf:ro
      - ./definitions.json:/etc/rabbitmq/definitions.json:ro
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "ping"]
      interval: 15s
      timeout: 10s
      retries: 5
      start_period: 30s

  prometheus:
    image: prom/prometheus:${PROMETHEUS_IMAGE_TAG:-v2.54.1}
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - ./monitoring/alerts.yml:/etc/prometheus/alerts.yml:ro
    depends_on:
      rabbitmq:
        condition: service_healthy

  node-exporter:
    image: prom/node-exporter:${NODE_EXPORTER_IMAGE_TAG:-v1.12.1}

  grafana:
    image: grafana/grafana:${GRAFANA_IMAGE_TAG:-11.1.0}
    volumes:
      - ./monitoring/grafana/provisioning:/etc/grafana/provisioning:ro
      - ./monitoring/grafana/grafana.ini:/etc/grafana/grafana.ini:ro
    depends_on:
      prometheus:
        condition: service_healthy

  producer:
    build: ./producer
    profiles: ["tools"]
    depends_on:
      rabbitmq:
        condition: service_healthy

  consumer:
    build: ./consumer
    profiles: ["tools"]
    depends_on:
      rabbitmq:
        condition: service_healthy
```

Полная версия со всеми портами, healthcheck-ами и volumes — см. файл `[docker-compose.yml](docker-compose.yml)` в корне репозитория.

### Файл `rabbitmq.conf`

См. `[rabbitmq.conf](rabbitmq.conf)`. Основные параметры:

```ini
prometheus.return_per_object_metrics = true
prometheus.tcp.port = 15692
management.tcp.port = 15672

vm_memory_high_watermark.relative = 0.4
vm_memory_high_watermark_paging_ratio = 0.5
disk_free_limit.absolute = 1GB

queue_master_locator = min-masters
management.load_definitions = /etc/rabbitmq/definitions.json
```

> ⚠️ В [Кейсе 7](#кейс-7-memorydisk-alarms--блокировка-producer) watermark временно понижается через `rabbitmqctl set_vm_memory_high_watermark` (без правки файла). Если всё же правите `rabbitmq.conf`, помните: `vm_memory_high_watermark.relative` и `.absolute` **взаимоисключающие** — оставляйте только один, иначе брокер не стартует.



### Файл `definitions.json`

Полностью — см. `[definitions.json](definitions.json)`. Объявляет:

- 4 exchange: `orders-direct`, `orders-fanout`, `orders-topic`, `orders-dlx`
- 7 очередей, включая `orders` (с `x-max-priority`, `x-dead-letter-exchange`) и `orders-dlq`
- Bindings, реализующие маршрутизацию по типу заказа (см. `[docs/concepts.md](docs/concepts.md)`)

Подробный разбор модели exchanges/queues/bindings — в `[docs/concepts.md](docs/concepts.md)`.

> ⚠️ Пользователь `guest`/`guest` в `definitions.json` задан через `password_hash`, а не открытым паролем — так требует формат definitions-файла. Если вы поменяете `RABBITMQ_USER`/`RABBITMQ_PASS` в своём `.env`, пересоздайте хэш: `docker compose exec rabbitmq rabbitmqctl hash_password '<новый-пароль>'` и подставьте результат в `definitions.json` (плюс переименуйте `name`/обновите `permissions.user`).

---



## Микросервис 1: Producer

`producer/producer.py` генерирует тестовые заказы и публикует их с учётом типа/приоритета:

- **Publisher confirms** включены по умолчанию (`channel.confirm_delivery()`), отключаются флагом `--no-confirms`.
- **Persistent-доставка** — `delivery_mode=2`.
- Аргументы очереди `orders` (`ORDERS_QUEUE_ARGS`) **выровнены** с `definitions.json`, чтобы повторное объявление очереди не приводило к ошибке `406 PRECONDITION_FAILED`.

```bash
docker compose run --rm producer python producer.py --count 20 --type mixed --delay 0.2
```


| Флаг            | Назначение                                |
| --------------- | ----------------------------------------- |
| `--count`       | Количество сообщений                      |
| `--delay`       | Задержка между сообщениями, сек           |
| `--type`        | `standard` / `express` / `bulk` / `mixed` |
| `--no-confirms` | Отключить publisher confirms              |


Полный код — `[producer/producer.py](producer/producer.py)`.

---



## Микросервис 2: Consumer

`consumer/consumer.py` потребляет `orders` с ручным ack, симулирует случайные сбои обработки (`FAILURE_RATE`) и retry-логику (`MAX_RETRIES`). После превышения лимита попыток сообщение публикуется в `orders-dlx` (см. `send_to_dlq()`).

DLQ-обработчик вынесен в отдельный модуль `[consumer/dlq_handler.py](consumer/dlq_handler.py)` — он запускается в фоновом потоке, слушает `orders-dlq` и логирует причину и время попадания в DLQ.


| ENV                                             | По умолчанию  | Назначение                                  |
| ----------------------------------------------- | ------------- | ------------------------------------------- |
| `PREFETCH_COUNT`                                | `5`           | Сколько сообщений consumer забирает заранее |
| `MAX_RETRIES`                                   | `3`           | Число попыток перед отправкой в DLQ         |
| `FAILURE_RATE`                                  | `0.15`        | Вероятность симулированного сбоя            |
| `PROCESSING_MIN_DELAY` / `PROCESSING_MAX_DELAY` | `0.5` / `2.0` | Диапазон задержки обработки, сек            |


```bash
docker compose run --rm -e PREFETCH_COUNT=1 -e FAILURE_RATE=0.5 consumer python consumer.py
```

Полный код — `[consumer/consumer.py](consumer/consumer.py)` и `[consumer/dlq_handler.py](consumer/dlq_handler.py)`.

---



## Настройка мониторинга

Полная теория, разбор метрик по уровням (queue/channel/node) и готовые PromQL-запросы — в `[docs/monitoring.md](docs/monitoring.md)`. Здесь — краткая шпаргалка.

### prometheus.yml

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  - "alerts.yml"

scrape_configs:
  - job_name: "rabbitmq"
    metrics_path: "/metrics"
    static_configs:
      - targets: ["rabbitmq:15692"]
  - job_name: "prometheus"
    static_configs:
      - targets: ["localhost:9090"]
  - job_name: "node-exporter"
    static_configs:
      - targets: ["node-exporter:9100"]
```



### Grafana: datasource и dashboard provisioning

`monitoring/grafana/provisioning/datasources/prometheus.yml` подключает Prometheus как datasource по умолчанию (с фиксированным `uid: prometheus`, чтобы дашборд мог сослаться на него надёжно).

`monitoring/grafana/provisioning/dashboards/rabbitmq.json` — это **«сырая» модель дашборда** (объект с полем `panels` на верхнем уровне), а не обёртка API-импорта `{"dashboard": ..., "overwrite": ...}`. Второй формат подходит только для `POST /api/dashboards/db`, но не читается file-provisioning механизмом Grafana.

Дашборд включает 8 панелей: глубина очереди `orders` (ready/unacked/total), publish rate, число consumer-ов, connections, память, свободное место на диске (`rabbitmq_disk_space_available_bytes` — нативная метрика брокера, а не `node_exporter`, у которого mountpoint внутри контейнера не совпадёт с host-путём named volume), глубина `orders-dlq` и суммарные publish/deliver/ack rates по каналам.

### Доступ к Grafana

1. Откройте `http://localhost:3000`, войдите как `admin` / `admin`.
2. **Dashboards → Browse → RabbitMQ → RabbitMQ Monitoring**.
3. Дашборд обновляется каждые 10 секунд и сразу показывает данные — Prometheus datasource подключён автоматически.



### Алертинг

Правила лежат в `[monitoring/alerts.yml](monitoring/alerts.yml)`: рост очереди, отсутствие consumer-ов, нехватка диска, высокая память. Посмотреть активные алерты — `http://localhost:9090/alerts`. Подробности — `[docs/monitoring.md](docs/monitoring.md#4-алертинг)`.

---



## Лабораторные кейсы

> Каждый кейс можно автоматически проверить: `./lab-solutions/case-N/verify.sh` (после выполнения шагов руками). Скрипты используют Management HTTP API и не заменяют ручное наблюдение в UI — только дополняют его.



### Кейс 1: Отправка и потребление сообщений (базовый поток)

**Цель:** Запустить producer и consumer, убедиться что сообщения доставляются.

> ⚠️ Consumer слушает только очередь `orders`. В неё попадают **только** заказы `--type standard` (через exchange `orders-direct`). По умолчанию producer шлёт `mixed` (`standard` / `express` / `bulk`) — тогда сообщения уйдут в `orders-topic-`* и `orders-fanout-*`, а `orders` останется пустой. Поэтому в этом кейсе явно указываем `--type standard`.

**Шаги:**

1. Запустите RabbitMQ и убедитесь что он healthy:

```bash
docker compose up -d rabbitmq
docker compose ps
```

1. Отправьте 5 сообщений **типа standard** (чтобы все попали в `orders`):

```bash
docker compose run --rm producer python producer.py --count 5 --type standard --delay 0.2
```

1. Проверьте в Management UI (`http://localhost:15672`) — вкладка **Queues** → очередь `orders` должна показать 5 сообщений (Ready).
2. Запустите consumer:

```bash
docker compose run --rm consumer python consumer.py
```

1. Наблюдайте в Management UI как сообщения уменьшаются: Ready: 0, Unacknowledged: 0.

**Ожидаемый результат:** Все 5 сообщений обработаны, очередь пуста.

**Автопроверка:** `./lab-solutions/case-1/verify.sh`

---



### Кейс 2: Влияние prefetch_count

**Цель:** Понять как `prefetch_count` влияет на распределение нагрузки.

**Шаги:**

1. Отправьте 20 сообщений в очередь `orders`:

```bash
docker compose run --rm producer python producer.py --count 20 --type standard --delay 0.1
```

1. Запустите consumer с `prefetch_count=1`:

```bash
docker compose run --rm -e PREFETCH_COUNT=1 consumer python consumer.py
```

1. Заметьте скорость обработки. Остановите consumer (Ctrl+C).
2. Повторите с `prefetch_count=10`:

```bash
docker compose run --rm producer python producer.py --count 20 --type standard --delay 0.1
docker compose run --rm -e PREFETCH_COUNT=10 consumer python consumer.py
```

1. Сравните:
  - **prefetch_count=1**: consumer обрабатывает по одному сообщению. Медленно, но безопасно.
  - **prefetch_count=10**: consumer берёт больше сообщений сразу. Быстрее, но выше нагрузка/риск при сбое.

**В Management UI:** вкладка **Channels** → `Unacked` — количество сообщений, взятых consumer-ом, но ещё не обработанных.

**Автопроверка:** `./lab-solutions/case-2/verify.sh 1` (пока запущен consumer с prefetch=1)

---



### Кейс 3: Dead Letter Queue — сбои и DLQ

**Цель:** Наблюдать как сообщения попадают в DLQ после `MAX_RETRIES` неудачных попыток.

**Шаги:**

1. Отправьте 30 сообщений в очередь `orders` (DLQ настроен именно на неё):

```bash
docker compose run --rm producer python producer.py --count 30 --type standard --delay 0.1
```

1. Запустите consumer с высокой вероятностью сбоя:

```bash
docker compose run --rm -e FAILURE_RATE=0.5 -e MAX_RETRIES=3 consumer python consumer.py
```

1. Наблюдайте в Management UI: очередь `orders` уменьшается; `orders-dlq` может кратко вырасти и снова стать пустой — встроенный DLQ-handler в consumer сразу забирает и логирует эти сообщения. Надёжный сигнал: логи `[WARNING] Message sent to DLQ` и `[WARNING] [consumer.dlq] DLQ: ...`.
2. В логах consumer вы увидите:

```
[ERROR] Failed to process order abc12345...: payment_timeout
[WARNING] Message sent to DLQ: order_id=abc12345... (after 3 attempts)
[WARNING] [consumer.dlq] DLQ: order_id=abc12345... reason=failed_after_3_retries
```

**Ожидаемый результат:** Часть сообщений обработана успешно, часть попала в DLQ (и была обработана DLQ-handler-ом).

**Автопроверка:** `./lab-solutions/case-3/verify.sh` (смотрит `message_stats.publish`/`ack`, а не текущую глубину — очередь к моменту проверки уже пуста)

---



### Кейс 4: Topic Exchange — маршрутизация по паттернам

**Цель:** Научиться маршрутизировать сообщения через topic exchange.

> ⚠️ В `orders-topic` попадают **только** заказы `--type express`. У `express` приоритет по умолчанию = 8 (≥ 7), поэтому без `--priority` **все** они уходят в `orders-topic-urgent`, а `orders-topic-regular` остаётся пустой. Чтобы увидеть обе очереди, форсируем приоритет явно.

**Шаги:**

1. Очистите хвосты прошлых прогонов (по желанию):

```bash
./scripts/cleanup.sh
```

1. Отправьте urgent-сообщения (`priority >= 7` → routing key `order.express.urgent`):

```bash
docker compose run --rm producer python producer.py --count 5 --type express --priority 9 --delay 0.1
```

1. Отправьте regular-сообщения (`priority < 7` → routing key `order.express.regular`):

```bash
docker compose run --rm producer python producer.py --count 5 --type express --priority 3 --delay 0.1
```

1. В Management UI, вкладка **Queues**:
  - `orders-topic-urgent`: Ready ≈ 5 (binding `order.*.urgent`)
  - `orders-topic-regular`: Ready ≈ 5 (binding `order.*.regular`)
2. (Опционально) Смешанный прогон — видно, что `standard`/`bulk` в topic **не** попадают:

```bash
docker compose run --rm producer python producer.py --count 20 --type mixed --delay 0.2
```

В UI вырастут `orders` / `orders-fanout-*`, а не только topic-очереди.

**Ожидаемый результат:** Express с высоким приоритетом → `orders-topic-urgent`, с низким → `orders-topic-regular`. Consumer для этого кейса не нужен (он слушает только `orders`).

**Автопроверка:** `./lab-solutions/case-4/verify.sh`

---



### Кейс 5: Priority Queues — приоритетная обработка

**Цель:** Сравнить обработку сообщений с разными приоритетами внутри одной очереди.

> ⚠️ В очередь `orders` попадают **только** заказы типа `standard` (см. `[docs/concepts.md](docs/concepts.md)` — `express` уходит в `orders-topic`, `bulk` — в `orders-fanout`). А приоритет `standard`-заказов всегда одинаковый (5), если не задать `--priority` явно. Поэтому для этого кейса используется флаг `--priority`, форсирующий конкретный приоритет независимо от типа заказа.

**Шаги:**

1. Остановите/не запускайте consumer заранее — сначала нужно накопить сообщения с разными приоритетами в одной очереди.
2. Отправьте сообщения с низким и высоким приоритетом (оба — `--type standard`, чтобы оба оказались в `orders`):

```bash
docker compose run --rm producer python producer.py --count 5 --type standard --priority 3 --delay 0.1
docker compose run --rm producer python producer.py --count 5 --type standard --priority 9 --delay 0.1
```

1. В Management UI → **Queues → orders → Get messages** (Ack mode: `Nack requeue`, Messages: 10) — посмотрите порядок: сообщения с приоритетом 9 должны идти раньше сообщений с приоритетом 3, независимо от порядка публикации.
2. Запустите consumer и убедитесь, что он тоже забирает высокоприоритетные сообщения первыми:

```bash
docker compose run --rm consumer python consumer.py
```

**Ожидаемый результат:** Сообщения с приоритетом 9 обрабатываются раньше сообщений с приоритетом 3.

**Автопроверка:** `./lab-solutions/case-5/verify.sh` (проверяет конфигурацию `x-max-priority`, порядок обработки — только визуально)

---



### Кейс 6: Масштабирование consumers — балансировка нагрузки

**Цель:** Наблюдать round-robin балансировку при нескольких consumer-ах.

**Шаги:**

1. Отправьте 30 сообщений в очередь `orders`:

```bash
docker compose run --rm producer python producer.py --count 30 --type standard --delay 0.1
```

1. Запустите 3 consumer-а в разных терминалах:

```bash
docker compose run --rm --name consumer-1 consumer python consumer.py
docker compose run --rm --name consumer-2 consumer python consumer.py
docker compose run --rm --name consumer-3 consumer python consumer.py
```

1. В Management UI → **Queues → orders → Consumers**: 3 потребителя, сообщения распределяются между ними.
2. Остановите один consumer — оставшиеся 2 забирают нагрузку.

**Ожидаемый результат:** При 3 consumer-ах обработка в ~3 раза быстрее; при остановке одного — автоматическая ребалансировка.

**Автопроверка:** `./lab-solutions/case-6/verify.sh` (пока запущено ≥ 2 consumer-ов)

---



### Кейс 7: Memory/Disk Alarms — блокировка producer

**Цель:** Наблюдать как RabbitMQ блокирует producer при memory alarm.

> ⚠️ Почему «налить 200k persistent-сообщений» обычно **не** поднимает RSS: `load-test` по умолчанию шлёт `delivery_mode=2` (persistent), а при `vm_memory_high_watermark_paging_ratio = 0.5` брокер сразу уводит сообщения на диск. В итоге в очереди могут лежать сотни мегабайт (`message_bytes`), а `messages_ram` ≈ 1 и **Memory Alarm не загорается**. Для демо надёжнее **опустить watermark ниже текущего RSS** — эффект тот же (`connection.blocked`), что и при реальном OOM-давлении.

**Шаги:**

1. Убедитесь, что consumer-ы остановлены (иначе очередь не растёт и картина смазывается):

```bash
docker ps -q --filter name=consumer-run | xargs -r docker stop
./scripts/cleanup.sh
```

2. Посмотрите текущее потребление памяти:

```bash
curl -s -u guest:guest http://localhost:15672/api/nodes | python3 -c "
import json, sys
n = json.load(sys.stdin)[0]
print(f\"used={n['mem_used']/1e6:.0f}MB limit={n['mem_limit']/1e6:.0f}MB alarm={n['mem_alarm']}\")
"
```

3. Опустите watermark ниже текущего RSS (alarm загорится сразу):

```bash
docker compose exec rabbitmq rabbitmqctl set_vm_memory_high_watermark absolute 100MB
```

4. Подтвердите alarm (в RabbitMQ 4.x поле `mem_alarm` на `/api/nodes` может оставаться `false` — смотрите health-check или Prometheus):

```bash
curl -s -u guest:guest http://localhost:15672/api/health/checks/alarms
# ожидаемо: HTTP 503 и {"alarms":[{"resource":"memory",...}], ...}

curl -s http://localhost:15692/metrics | grep rabbitmq_alarms_memory_used_watermark
# ожидаемо: rabbitmq_alarms_memory_used_watermark 1
```

В Management UI → **Overview** тоже может появиться баннер Memory alarm; если UI «молчит», ориентируйтесь на команды выше — alarm при этом уже действует и блокирует publisher'ов.

5. Запустите нагрузку — publisher'ы упрутся в `connection.blocked` (подтверждения зависнут / упадут по timeout):

```bash
docker compose run --rm \
  -v "$(pwd)/scripts:/app/scripts:ro" \
  producer python scripts/load-test.py --total 5000 --workers 5 --size 2048
```

6. Снимите alarm и верните относительный лимит из `rabbitmq.conf`:

```bash
docker compose exec rabbitmq rabbitmqctl set_vm_memory_high_watermark 0.4
```

**Ожидаемый результат:** При активном memory alarm publish блокируется; после `set_vm_memory_high_watermark 0.4` — снова проходит.

**Автопроверка:** `./lab-solutions/case-7/verify.sh` (запускайте **пока** watermark ещё понижен, шаг 3–5).

---



### Кейс 8: Симуляция кластера — 2 узла с quorum queues

**Цель:** Поднять 2-узловой кластер, объявить quorum queue и **на практике** увидеть leader/replicas и поведение при потере узла.

> ⚠️ Учебный кластер из **2 узлов**. Для quorum majority = `N/2+1`, то есть **2 из 2**. Падение любого узла лишает очередь кворума — publish перестаёт проходить. В проде ставят **3+** узлов (переживают падение одного). Это как раз то, что демонстрирует кейс.

**Шаги:**

1. Остановите основной стенд, если он запущен (порты 5672/15672/15692 совпадают):

```bash
docker compose down
```

2. Запустите кластер:

```bash
docker compose -f docker-compose.cluster.yml up -d
```

3. Подключите второй узел к кластеру:

```bash
docker exec -it rabbitmq-2 rabbitmqctl stop_app
docker exec -it rabbitmq-2 rabbitmqctl reset
docker exec -it rabbitmq-2 rabbitmqctl join_cluster rabbit@rabbitmq-1
docker exec -it rabbitmq-2 rabbitmqctl start_app
```

4. Проверьте кластер:

```bash
docker exec -it rabbitmq-1 rabbitmqctl cluster_status
```

5. Создайте quorum queue через `rabbitmqadmin` (входит в образ `*-management`; в RabbitMQ 4.x CLI v2 — флаги `--name`/`--type`, а не старый стиль `name=...`):

```bash
docker exec -it rabbitmq-1 rabbitmqadmin declare queue \
  --name orders-quorum --type quorum --durable true
```

6. Посмотрите leader и реплики:

```bash
docker exec -it rabbitmq-1 rabbitmqctl list_queues name type leader members online
```

Ожидаемо: `type=quorum`, `members` и `online` содержат и `rabbit@rabbitmq-1`, и `rabbit@rabbitmq-2`.

7. Опубликуйте несколько сообщений (default exchange → routing key = имя очереди):

```bash
for i in 1 2 3 4 5; do
  docker exec rabbitmq-1 rabbitmqadmin publish message \
    --routing-key orders-quorum \
    --payload "{\"order_id\":\"quorum-$i\"}"
done

docker exec rabbitmq-1 rabbitmqctl list_queues name messages leader members online
```

В Management UI (`http://localhost:15672`) → **Queues → orders-quorum**: type `quorum`, Ready ≈ 5, видны node / members.

8. Остановите follower и посмотрите, что quorum пропал:

```bash
# кто сейчас leader — останавливаем другой узел (если leader = rabbitmq-1, стопаем rabbitmq-2)
docker stop rabbitmq-2

docker exec rabbitmq-1 rabbitmqctl list_queues name messages members online state
```

`online` сузится до одного узла. Сообщения, уже зафиксированные majority, **остаются** в очереди.

9. Попробуйте опубликовать ещё одно сообщение — без majority publish не проходит (команда упадёт / зависнет по таймауту):

Linux:

```bash
timeout 15 docker exec rabbitmq-1 rabbitmqadmin publish message \
  --routing-key orders-quorum \
  --payload '{"order_id":"should-fail-without-quorum"}' || echo "publish failed as expected (no quorum)"
```

macOS (нет GNU `timeout`; используем `perl`):

```bash
perl -e 'alarm 15; exec @ARGV' docker exec rabbitmq-1 rabbitmqadmin publish message \
  --routing-key orders-quorum \
  --payload '{"order_id":"should-fail-without-quorum"}' || echo "publish failed as expected (no quorum)"
```

10. Верните узел и убедитесь, что кластер и очередь снова online:

```bash
docker start rabbitmq-2
# подождите healthy (10–20 с)
docker exec rabbitmq-1 rabbitmqctl list_queues name messages members online
```

Сообщения на месте, оба member снова в `online`. Повторите publish — снова успех.

**Ожидаемый результат:** Quorum queue реплицируется на оба узла; при одном живом узле из двух publish блокируется (нет majority); после возврата узла очередь снова принимает сообщения. Вывод для прода: **нечётное число узлов ≥ 3**.

**Автопроверка:** `./lab-solutions/case-8/verify.sh` (оба узла должны быть up)

Теория по Raft-консенсусу, partition handling и командам `rabbitmqctl` — [`docs/clustering.md`](docs/clustering.md).

---



## Кейсы для отладки



### Кейс 1: «Producer blocked» — disk/memory alarm

**Симптом:** Producer не может отправить сообщения, логи показывают `ConnectionBlocked`.

**Диагностика:**

```bash
curl -u guest:guest http://localhost:15672/api/nodes | python3 -m json.tool
```

Проверьте поле `alarms` в ответе — непустой список означает активный alarm.

**Решение:**

```bash
# Disk alarm — освободите место или увеличьте disk_free_limit.absolute в rabbitmq.conf
docker system prune -a -f
docker compose restart rabbitmq

# Memory alarm — увеличьте vm_memory_high_watermark.relative, уменьшите prefetch_count
# у consumer-ов, либо отключите часть подключений через Management UI (Connections).
```

---



### Кейс 2: «Messages stuck in queue» — consumer не ack-ит

**Симптом:** Очередь `orders` содержит сообщения, consumer запущен, но сообщения не уменьшаются.

**Диагностика:**

```bash
curl -u guest:guest http://localhost:15672/api/queues/%2F/orders | python3 -c "
import sys, json
data = json.load(sys.stdin)
print(f'Consumers: {data[\"consumers\"]}')
print(f'Unacked: {data[\"messages_unacknowledged\"]}')
print(f'Ready: {data[\"messages_ready\"]}')
"
```

**Решение:**

- Если `prefetch_count` слишком высокий — уменьшите его (`-e PREFETCH_COUNT=5`).
- Если consumer завис — перезапустите его.
- Убедитесь, что в коде используется `auto_ack=False` (см. `consumer/consumer.py`).
- Проверьте, что consumer вообще запущен: `docker compose ps | grep consumer`.

---



### Кейс 3: «Queue grows indefinitely» — нет TTL, нет DLX

**Симптом:** Очередь растёт бесконечно, consumer-ов нет или они не справляются.

**Диагностика:**

```bash
curl -u guest:guest http://localhost:15672/api/queues/%2F/orders | python3 -c "
import sys, json
data = json.load(sys.stdin)
print(f'Messages: {data[\"messages\"]}, Consumers: {data[\"consumers\"]}')
print(f'Arguments: {json.dumps(data[\"arguments\"], indent=2)}')
"
```

**Решение:**

```bash
# Добавить TTL (5 минут) и DLX через policy
curl -u guest:guest -X PUT http://localhost:15672/api/policies/%2F/ttl-and-dlx \
  -H "content-type: application/json" \
  -d '{
    "pattern": "^orders$",
    "definition": {"message-ttl": 300000, "dead-letter-exchange": "orders-dlx"},
    "priority": 10,
    "apply-to": "queues"
  }'

# Или очистить очередь: ./scripts/cleanup.sh
```

---



### Кейс 4: «Cluster partition» — network split

**Симптом:** В Management UI видно "Partitioned"-состояние, узлы не видят друг друга.

**Диагностика:**

```bash
docker exec -it rabbitmq-1 rabbitmqctl cluster_status | grep -A 5 "partitions"
```

**Решение:**

- `cluster_partition_handling = autoheal` — для 2-узловых кластеров.
- `cluster_partition_handling = pause_minority` — для 3+ узлов.
- Ручное восстановление: `stop_app` → `reset` → `join_cluster` → `start_app` на «отвалившемся» узле.

Подробности — `[docs/clustering.md](docs/clustering.md#5-partition-handling--что-происходит-при-split-пары-узлов)`.

---



### Кейс 5: «Leader переключился неожиданно» — quorum queue rebalancing

**Симптом:** После изменений в кластере Leader quorum-очереди сменился без видимой причины.

**Диагностика:**

```bash
curl -u guest:guest http://localhost:15672/api/queues/%2F/orders-quorum | python3 -c "
import sys, json
data = json.load(sys.stdin)
print(f'Leader: {data.get(\"leader\", \"N/A\")}, Online: {data.get(\"online\", [])}')
"
```

**Решение:**

- Это нормальное поведение Raft — leader меняется автоматически при перебалансировке.
- Для предсказуемого размещения используйте `queue_master_locator = min-masters` (уже задано в `rabbitmq.conf`).
- Принудительный transfer лидерства выполняется через `rabbitmq-diagnostics`/`rabbitmqctl` — обычно не требуется в норме.

---



## Бонусные кейсы



### Бонус 1: TLS для RabbitMQ соединений (mutual TLS)

**Цель:** Включить шифрование AMQP на порту `5671` и **взаимную** проверку сертификатов (mTLS): брокер проверяет клиентский сертификат, клиент проверяет сертификат брокера.

**Что получится на выходе**

```text
Producer/Consumer --TLS+client cert--> :5671 RabbitMQ
                 (порт 5672 пока остаётся открытым для остальных кейсов)
```

| Роль | Файл | Зачем |
|------|------|--------|
| CA | `tls/ca.pem` (+ `ca.key`) | Кем подписаны server/client; клиент и брокер доверяют этой CA |
| Server | `tls/server.pem` + `server.key` | Идентичность брокера (`CN=rabbitmq`, SAN: `rabbitmq`, `localhost`, `127.0.0.1`) |
| Client | `tls/client.pem` + `client.key` | Идентичность клиента; без него брокер отвергнет connect (`fail_if_no_peer_cert = true`) |

---

#### Шаг 0. Предусловия

- Основной стенд (не cluster) доступен: `docker compose up -d rabbitmq`
- Установлен `openssl` (`openssl version`)
- Для smoke-теста с хоста: `pip install -r producer/requirements.txt`  
  либо запускайте тест через контейнер `producer` (см. шаг 6)

Если сейчас запущен cluster-compose — сначала `docker compose -f docker-compose.cluster.yml down`, затем обычный `docker compose up -d`.

---

#### Шаг 1. Сгенерировать сертификаты

```bash
./scripts/generate-tls-certs.sh
ls -la tls/
```

Скрипт создаёт одноразовый CA, server и client cert в `tls/` (в `.gitignore` — в git не попадёт).

**Что происходит:** `openssl` выпускает собственный CA и подписывает им server/client. В server-сертификат добавляются SAN, чтобы hostname verification работал и с хоста (`localhost`), и из Docker-сети (`rabbitmq`).

---

#### Шаг 2. Включить TLS в `rabbitmq.conf`

В конец [`rabbitmq.conf`](rabbitmq.conf) **добавьте** (не удаляя остальное):

```ini
# --- Bonus 1: TLS / mTLS (AMQP on 5671) ---
listeners.ssl.default = 5671
ssl_options.cacertfile = /etc/rabbitmq/ssl/ca.pem
ssl_options.certfile   = /etc/rabbitmq/ssl/server.pem
ssl_options.keyfile    = /etc/rabbitmq/ssl/server.key
ssl_options.verify     = verify_peer
ssl_options.fail_if_no_peer_cert = true
```

**Что означают опции**

| Опция | Смысл |
|-------|--------|
| `listeners.ssl.default = 5671` | Брокер слушает AMQP-over-TLS на 5671 |
| `cacertfile` | CA, которой брокер доверяет при проверке client cert |
| `certfile` / `keyfile` | Собственный сертификат и ключ брокера |
| `verify = verify_peer` | Требовать и проверять сертификат клиента |
| `fail_if_no_peer_cert = true` | Без client cert — отказ (настоящий mTLS) |

> Обычный `listeners` на `5672` **не отключаем** — остальные лабораторные кейсы продолжают работать. В проде часто ставят `listeners.tcp = none`, оставляя только TLS.

---

#### Шаг 3. Смонтировать сертификаты и открыть порт 5671

Используйте overlay [`docker-compose.tls.yml`](docker-compose.tls.yml) — он только добавляет volume `./tls → /etc/rabbitmq/ssl` и публикует порт `5671`:

```bash
docker compose -f docker-compose.yml -f docker-compose.tls.yml up -d rabbitmq
docker compose ps rabbitmq
```

Проверьте, что брокер healthy и слушает 5671:

```bash
docker compose logs rabbitmq 2>&1 | grep -i -E 'ssl|tls|5671|started TCP listener' | tail -20
docker compose exec rabbitmq rabbitmq-diagnostics listeners
```

В выводе `listeners` должны быть и `amqp` на 5672, и `amqp/ssl` на 5671.

---

#### Шаг 4. Проверка: с сертификатами — да, без — нет

**4a. openssl (только доверие к server cert)**

```bash
openssl s_client -connect localhost:5671 -CAfile tls/ca.pem \
  -cert tls/client.pem -key tls/client.key -brief </dev/null
```

Ожидаемо: `CONNECTION ESTABLISHED`, `Verification: OK`, `Peer certificate: CN=rabbitmq`.

> ⚠️ `Verification: OK` значит лишь то, что **клиент** принял **серверный** сертификат. Это ещё не mTLS.

Без `-cert/-key` openssl часто всё равно пишет `CONNECTION ESTABLISHED` (путаница TLS 1.3/`-brief`). Отказ брокера смотрите в логах:

```bash
openssl s_client -connect localhost:5671 -CAfile tls/ca.pem -brief </dev/null
docker compose logs rabbitmq --since 1m | grep -i certificate_required
# ожидаемо: Fatal - Certificate Required
```

**4b. Реальная проверка mTLS через pika** (здесь уже видна ошибка без client cert)

Обязательно с TLS-overlay, иначе `compose run` может пересоздать брокер без mount `./tls`:

```bash
docker compose -f docker-compose.yml -f docker-compose.tls.yml run --rm \
  -v "$(pwd)/scripts:/app/scripts:ro" \
  -v "$(pwd)/tls:/certs:ro" \
  -e RABBITMQ_HOST=rabbitmq \
  -e TLS_CA=/certs/ca.pem \
  -e TLS_CERT=/certs/client.pem \
  -e TLS_KEY=/certs/client.key \
  producer python scripts/tls-smoke-test.py --negative
```

Ожидаемый вывод:

```text
[PASS] mTLS on 5671: connected, published and consumed b'{"ok":true,"via":"tls-smoke-test"}'
--- negative checks (expected FAIL) ---
[FAIL] TLS without client cert: ... TLSV13_ALERT_CERTIFICATE_REQUIRED ...
[PASS] broker rejected connection without client cert (as expected)
[INFO] plain 5672 still works — expected unless you set listeners.tcp = none
```

Итого: **с** `client.pem`/`client.key` — publish/consume на `5671` проходит; **без** них — `TLSV13_ALERT_CERTIFICATE_REQUIRED`. Строка `[FAIL] TLS without client cert` здесь как раз ожидаема и означает, что mTLS настроен правильно.

В Management UI (`http://localhost:15672`) → **Connections** у успешной сессии будут детали SSL/TLS.

---

#### Шаг 5. Подключение из Python (pika) — как это выглядит в коде

```python
import ssl
import pika

context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
context.minimum_version = ssl.TLSVersion.TLSv1_2
context.load_verify_locations("tls/ca.pem")
context.load_cert_chain("tls/client.pem", "tls/client.key")
context.verify_mode = ssl.CERT_REQUIRED
context.check_hostname = True

params = pika.ConnectionParameters(
    host="localhost",          # из контейнера producer — host="rabbitmq"
    port=5671,
    credentials=pika.PlainCredentials("guest", "guest"),
    ssl_options=pika.SSLOptions(context, server_hostname="localhost"),
)

connection = pika.BlockingConnection(params)
channel = connection.channel()
print("mTLS OK")
connection.close()
```

**Что здесь важно:** шифрование (TLS) и логин/пароль (PLAIN `guest`/`guest`) — разные слои. mTLS доказывает «кто на том конце провода»; username/password по-прежнему авторизуют в vhost.

---

#### Шаг 6. (Опционально) Повтор smoke-теста с хоста

Если хотите гонять тот же скрипт локально (нужен venv + `pip install -r producer/requirements.txt`):

```bash
python scripts/tls-smoke-test.py --negative
```

Иначе достаточно шага **4b**.

---

#### Шаг 7. (Опционально) Producer по TLS

Одноразовый publish через тот же SSL context можно встроить в свой скрипт; для лабы достаточно smoke-теста. Если гоняете штатный `producer.py`, ему нужны правки (`ssl_options` + порт 5671) — он из коробки ходит на plain `5672`.

---

#### Шаг 8. Откат (вернуть обычный стенд)

1. Удалите или закомментируйте блок `# --- Bonus 1: TLS` в `rabbitmq.conf`.
2. Поднимите брокер **без** TLS-overlay:

```bash
docker compose up -d rabbitmq
```

3. (По желанию) удалите локальные сертификаты: `rm -rf tls/`

Порт 5671 перестанет слушаться; кейсы 1–7 снова только на `5672`.

---

**Ожидаемый результат:** AMQP-over-TLS на `5671` с обязательным client certificate; smoke-тест `[PASS]`; connect без client cert — отказ; plain `5672` в этой лабе остаётся для остальных упражнений.

**Типичные проблемы**

| Симптом | Причина / что сделать |
|---------|------------------------|
| Брокер не стартует, в логах `ssl_options` / file not found | Нет mount `./tls` или забыли `generate-tls-certs.sh` — используйте `docker-compose.tls.yml` |
| `CERTIFICATE_VERIFY_FAILED` / hostname mismatch | Подключаетесь не к тому host; в SAN есть `localhost` и `rabbitmq` — совпадите `RABBITMQ_HOST` / `server_hostname` |
| Handshake OK в openssl, pika падает | Проверьте пути к `ca/client` и что `SSLOptions(..., server_hostname=...)` совпадает с host |
| Permission denied на `.key` | Перегенерируйте сертификаты скриптом (он ставит `chmod 644`) |

---



### Бонус 2: Shovel plugin для кросс-кластерной репликации

**Цель:** Настроить Shovel для передачи сообщений между двумя RabbitMQ кластерами.

```bash
docker exec -it rabbitmq-1 rabbitmq-plugins enable rabbitmq_shovel rabbitmq_shovel_management

curl -u guest:guest -X PUT \
  http://localhost:15672/api/parameters/shovel/%2F/orders-shovel \
  -H "content-type: application/json" \
  -d '{
    "src-protocol": "amqp091",
    "src-uri": "amqp://guest:guest@rabbitmq-1:5672",
    "src-queue": "orders",
    "dest-protocol": "amqp091",
    "dest-uri": "amqp://guest:guest@rabbitmq-2:5672",
    "dest-queue": "orders-replicated",
    "ack-mode": "on-confirm"
  }'
```

Проверка: **Admin → Shovels** в Management UI, статус должен быть `Running`.

---



### Бонус 3: Federation для мульти-датацентрной симуляции

**Цель:** Настроить Federation для федерации очередей между «датацентрами».

```bash
docker exec -it rabbitmq-1 rabbitmq-plugins enable rabbitmq_federation rabbitmq_federation_management

curl -u guest:guest -X PUT \
  http://localhost:15672/api/parameters/federation-upstream/%2F/dc2-upstream \
  -H "content-type: application/json" \
  -d '{
    "uri": "amqp://guest:guest@rabbitmq-2:5672",
    "prefetch-count": 1000,
    "reconnect-delay": 5,
    "ack-mode": "on-confirm"
  }'

curl -u guest:guest -X PUT \
  http://localhost:15672/api/policies/%2F/federation-policy \
  -H "content-type: application/json" \
  -d '{
    "pattern": "^federated\\.",
    "definition": {"federation-upstream": "dc2-upstream"},
    "priority": 10,
    "apply-to": "queues"
  }'
```

---



## Production: как надо и как не надо

> Быстрая шпаргалка о том, **что делать нужно**, **что нельзя**, и **какие практики являются антипаттернами** при эксплуатации RabbitMQ в продакшене.



### ✅ Что НУЖНО делать



#### Архитектура и кластеризация


| Практика                               | Почему важно                                                                                      |
| -------------------------------------- | ------------------------------------------------------------------------------------------------- |
| Минимум 3 узла в кластере              | Quorum Queues требуют большинства (2 из 3, 3 из 5). На 2 узлах при partition теряется доступность |
| Quorum Queues вместо Classic queues    | Raft-консенсус, устойчивость к partition. Classic mirrored queues удалены в RabbitMQ 4.x          |
| Нечётное количество узлов              | Предотвращает split-brain: при 3 узлах — 2 здоровых из 3 = кластер жив                            |
| Узлы в разных AZ/зонах                 | Выживание при аварии одной зоны                                                                   |
| Отдельные диски для Mnesia и msg_store | Снижает contention между метаданными и данными сообщений                                          |




#### Конфигурация (production-профиль)

```ini
queue_master_locator = min-masters
vm_memory_high_watermark.relative = 0.6
disk_free_limit.absolute = 5GB

tcp.listen_options.backlog = 256
tcp.listen_options.nodelay = true

cluster_partition_handling = pause_minority

management.listener.ssl = true
prometheus.return_per_object_metrics = true
```



#### Безопасность


| Практика                    | Реализация                                                        |
| --------------------------- | ----------------------------------------------------------------- |
| Включи TLS                  | `listeners.ssl.default = 5671` + сертификаты (см. Бонус 1)        |
| Удали guest-аккаунт         | `rabbitmqctl delete_user guest`                                   |
| Отдельный user на сервис    | Не используй одного user для всех producers/consumers             |
| ACL                         | Ограничь доступ к очередям по vhost и routing key                 |
| Firewall на management-порт | 15672 — только для admin-подсети                                  |
| Vhost на каждый env         | dev/staging/prod — разные vhost-ы (см. `scripts/create-vhost.sh`) |


```bash
rabbitmqctl add_user app_producer "$(openssl rand -base64 24)"
rabbitmqctl set_permissions -p /production app_producer ".*" "" ""   # только publish

rabbitmqctl add_user app_consumer "$(openssl rand -base64 24)"
rabbitmqctl set_permissions -p /production app_consumer "" ".*" ""   # только consume
```



#### Мониторинг и алерты


| Метрика                                  | Порог алерта                                    |
| ---------------------------------------- | ----------------------------------------------- |
| `rabbitmq_queue_messages`                | > 10 000 или растёт быстрее 1000/5 мин          |
| `rabbitmq_queue_consumers`               | = 0 дольше 2 минут                              |
| `rabbitmq_process_resident_memory_bytes` | > 80% от `rabbitmq_resident_memory_limit_bytes` |
| `rabbitmq_disk_space_available_bytes`    | < 2GB                                           |
| `rabbitmq_connections`                   | Резкий скачок (> 2x от нормы)                   |


Готовые правила — `[monitoring/alerts.yml](monitoring/alerts.yml)`, теория — `[docs/monitoring.md](docs/monitoring.md)`.

#### Backup и Recovery


| Практика            | Как делать                                                                                         |
| ------------------- | -------------------------------------------------------------------------------------------------- |
| Бэкап definitions   | `rabbitmqadmin export /backup/definitions-$(date +%Y%m%d).json` ежедневно через cron               |
| Бэкап Mnesia        | `rabbitmqctl stop_app && rsync /var/lib/rabbitmq/mnesia/ /backup/mnesia/ && rabbitmqctl start_app` |
| Бэкап Erlang Cookie | Без cookie не восстановить кластер — храните в Vault/Secrets Manager                               |
| Runbook             | Документируйте: add node, remove node, recover from partition, recover from disk failure           |


---



### ❌ Чего НЕЛЬЗЯ делать



#### Антипаттерны конфигурации


| ❌ Антипаттерн                                         | Почему плохо                                     | ✅ Правильно                              |
| ----------------------------------------------------- | ------------------------------------------------ | ---------------------------------------- |
| `prefetch_count = 0` (безлимитный)                    | Один consumer забирает всё, риск OOM             | `prefetch_count = 5-50`                  |
| `auto_ack = true` в production                        | Потеря сообщений при crash consumer-а            | `auto_ack = false` + явный `basic_ack()` |
| Classic Mirror Queues                                 | Удалены в RabbitMQ 4.x, split-brain risk         | Quorum Queues                            |
| Статический `peer_discovery_backend = classic_config` | Нужно менять конфиг при каждом узле              | K8s API / Consul / DNS discovery         |
| Нет `x-dead-letter-exchange`                          | «Битые» сообщения циркулируют бесконечно         | DLX + DLQ на каждый сервис               |
| Один exchange для всех сервисов                       | Маршрутизация неуправляема                       | Изолированные exchanges per service      |
| Нет message TTL                                       | Очередь растёт бесконечно при падении consumer-а | `x-message-ttl` или policy               |




#### Антипаттерны эксплуатации


| ❌ Антипаттерн                            | Что происходит                   | ✅ Правильно                                                |
| ---------------------------------------- | -------------------------------- | ---------------------------------------------------------- |
| `rabbitmqctl force_boot` без понимания   | Риск потери данных при partition | Сначала `cluster_status`, force_boot — последняя мера      |
| Ручное перемещение очередей между узлами | Нарушает балансировку            | `queue_master_locator = min-masters`                       |
| Удаление узла без `forget_cluster_node`  | «Призрачный» узел в кластере     | `rabbitmqctl forget_cluster_node <node>` перед выключением |
| Игнорирование memory alarms              | Producer-ы блокируются           | Alerts + автоскейлинг                                      |
| Диск с < 5GB свободного места            | Disk alarm → блокировка → outage | Мониторинг + алерт при < 10GB                              |
| Один vhost для всех окружений            | Dev может повлиять на prod       | Отдельный vhost на env                                     |




#### Антипаттерны кода

**Producer — неправильно:**

```python
# Без publisher confirms — сообщения могут потеряться при сбое брокера
channel.basic_publish(exchange="orders", routing_key="new", body=json.dumps(order))
```

**Producer — правильно** (см. `producer/producer.py`):

```python
channel.confirm_delivery()
try:
    channel.basic_publish(
        exchange="orders-direct",
        routing_key="order.new",
        body=json.dumps(order).encode(),
        properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"),
    )
except pika.exceptions.UnroutableError:
    logger.error("Message rejected — queue not available")
except pika.exceptions.NackError:
    logger.error("Message nacked by broker")
```

**Consumer — неправильно:**

```python
# auto_ack=True + нет обработки ошибок — при сбое сообщение теряется навсегда
for method, properties, body in channel.consume("orders", auto_ack=True):
    process(body)
```

**Consumer — правильно** (см. `consumer/consumer.py` + `consumer/dlq_handler.py`):

```python
def callback(ch, method, properties, body):
    retry_count = (properties.headers or {}).get("x-retry-count", 0)
    if process_order(body):
        ch.basic_ack(delivery_tag=method.delivery_tag)
    elif retry_count + 1 >= MAX_RETRIES:
        send_to_dlq(ch, body, properties, retry_count + 1)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    else:
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
```

---



### 🔥 Production Readiness Checklist

- [ ] **Кластер:** минимум 3 узла, quorum queues, `cluster_partition_handling = pause_minority`
- [ ] **Диск:** > 20GB свободного, `disk_free_limit.absolute = 5GB`, SSD рекомендован
- [ ] **Память:** `vm_memory_high_watermark.relative = 0.6`, мониторинг RSS
- [ ] **TLS:** включён на порту 5671, management UI за HTTPS
- [ ] **Пользователи:** guest удалён, отдельные users для каждого сервиса, ACL настроены
- [ ] **Vhosts:** изолированные per env (dev/staging/prod)
- [ ] **Queues:** quorum queues, DLX настроен, message TTL задан
- [ ] **Producer:** publisher confirms включены, `delivery_mode=2`
- [ ] **Consumer:** `prefetch_count` задан, manual ack, retry + DLQ
- [ ] **Мониторинг:** Prometheus + Grafana, алерты на queue depth / consumers / disk / memory
- [ ] **Бэкап:** definitions экспортируются ежедневно, Mnesia бэкапится
- [ ] **Runbook:** документированы процедуры add node / remove node / recover from partition / recover from disk failure

---



## Чеклист для собеседования



### Mid-level DevOps

- [ ] Основы AMQP: exchanges (direct, fanout, topic), queues, bindings, routing keys
- [ ] Приоритетные очереди: как настроить и когда использовать
- [ ] Dead Letter Exchange: зачем нужен, как настроить через policy и arguments
- [ ] Prefetch count: влияние на потребление, балансировка нагрузки
- [ ] Publisher confirms: гарантии доставки
- [ ] Consumer acknowledgements: auto_ack vs manual ack, nack с requeue
- [ ] Мониторинг: ключевые метрики (queue depth, message rate, consumer count)
- [ ] Docker: запуск RabbitMQ через Docker Compose, healthchecks
- [ ] Management UI: навигация, просмотр очередей, consumer-ов, каналов



### Senior DevOps

- [ ] Quorum Queues: зачем нужны, как работают (Raft), отличия от classic queues
- [ ] Кластеризация: join_cluster, cluster_status, Erlang cookie, partitions
- [ ] Partition handling: autoheal vs pause_minority, когда что использовать
- [ ] Memory/Disk alarms: watermark, disk_free_limit, production-настройки
- [ ] TLS: настройка шифрования, mutual TLS, управление сертификатами
- [ ] Shovel и Federation: кросс-кластерная репликация, multi-datacenter
- [ ] Retention policies: message TTL, queue TTL, max-length, overflow strategies
- [ ] Prometheus integration: настройка метрик, кастомные дашборды
- [ ] Инцидент-менеджмент: диагностика partition, recovery procedures, runbook



### Архитектурные вопросы

- [ ] RabbitMQ vs Kafka: когда что выбирать, trade-offs
- [ ] Паттерны: competing consumers, pub/sub, request-reply, RPC
- [ ] Планирование capacity: сколько сообщений в секунду, memory footprint
- [ ] HA стратегии: quorum queues, mirrored queues (legacy), master locator
- [ ] Observability: structured logging, distributed tracing, correlation IDs

---



## Дополнительные материалы

- `[docs/concepts.md](docs/concepts.md)` — теория: exchanges, queues, bindings, routing key, DLX
- `[docs/clustering.md](docs/clustering.md)` — теория: кластеризация, Raft, quorum queues, partition handling
- `[docs/monitoring.md](docs/monitoring.md)` — теория: метрики по уровням (queue/channel/node), PromQL, алерты

---

> **Актуально на июль 2026**
> Руководство подготовлено для RabbitMQ 4.1 (management) с использованием Docker Compose v2, Python 3.12, Prometheus v2.54.1, Node Exporter v1.12.1 и Grafana 11.1.0.

