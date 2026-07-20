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
- Теоретические материалы в [`docs/`](docs/)

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

| Компонент | Версия | Назначение |
|-----------|--------|------------|
| **RabbitMQ** | 4.1 (management) | Брокер сообщений |
| **Docker Compose** | v2 | Оркестрация контейнеров |
| **Python** | 3.12 | Язык микросервисов |
| **pika** | 1.3.2 | Python-клиент RabbitMQ |
| **Prometheus** | v2.54.1 | Сбор метрик |
| **Grafana** | 11.1.0 | Визуализация метрик |
| **Node Exporter** | v1.12.1 | Метрики системы |

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

Значения по умолчанию в [`.env.example`](.env.example) (учётные данные, версии образов, порты) уже готовы для локального обучения — менять их не обязательно. Если вы пропустите этот шаг и запустите `./scripts/setup.sh`, он создаст `.env` за вас автоматически.

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

| Сервис | URL | Логин / Пароль |
|--------|-----|-----------------|
| RabbitMQ Management | http://localhost:15672 | `guest` / `guest` |
| Grafana | http://localhost:3000 | `admin` / `admin` |
| Prometheus | http://localhost:9090 | — |

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
./scripts/cleanup.sh          # покурить очереди orders/orders-dlq, стенд не останавливается
./scripts/cleanup.sh --full   # docker compose down -v — полный сброс
```

---

## docker-compose.yml

Полный файл лежит в [`docker-compose.yml`](docker-compose.yml). Ключевые моменты:

- Все креды/версии/порты берутся из `.env` (создаётся из [`.env.example`](.env.example)) через `${VAR:-default}` — не хардкодятся.
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

Полная версия со всеми портами, healthcheck-ами и volumes — см. файл [`docker-compose.yml`](docker-compose.yml) в корне репозитория.

### Файл `rabbitmq.conf`

См. [`rabbitmq.conf`](rabbitmq.conf). Основные параметры:

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

> ⚠️ В [Кейсе 7](#кейс-7-memorydisk-alarms--блокировка-producer) вы временно замените `vm_memory_high_watermark.relative` на `vm_memory_high_watermark.absolute`. Эти два параметра **взаимоисключающие** — если оставить оба в файле одновременно, RabbitMQ не запустится с ошибкой конфигурации. Всегда **заменяйте**, а не дополняйте.

### Файл `definitions.json`

Полностью — см. [`definitions.json`](definitions.json). Объявляет:

- 4 exchange: `orders-direct`, `orders-fanout`, `orders-topic`, `orders-dlx`
- 7 очередей, включая `orders` (с `x-max-priority`, `x-dead-letter-exchange`) и `orders-dlq`
- Bindings, реализующие маршрутизацию по типу заказа (см. [`docs/concepts.md`](docs/concepts.md))

Подробный разбор модели exchanges/queues/bindings — в [`docs/concepts.md`](docs/concepts.md).

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

| Флаг | Назначение |
|------|-----------|
| `--count` | Количество сообщений |
| `--delay` | Задержка между сообщениями, сек |
| `--type` | `standard` / `express` / `bulk` / `mixed` |
| `--no-confirms` | Отключить publisher confirms |

Полный код — [`producer/producer.py`](producer/producer.py).

---

## Микросервис 2: Consumer

`consumer/consumer.py` потребляет `orders` с ручным ack, симулирует случайные сбои обработки (`FAILURE_RATE`) и retry-логику (`MAX_RETRIES`). После превышения лимита попыток сообщение публикуется в `orders-dlx` (см. `send_to_dlq()`).

DLQ-обработчик вынесен в отдельный модуль [`consumer/dlq_handler.py`](consumer/dlq_handler.py) — он запускается в фоновом потоке, слушает `orders-dlq` и логирует причину и время попадания в DLQ.

| ENV | По умолчанию | Назначение |
|-----|---------------|-----------|
| `PREFETCH_COUNT` | `5` | Сколько сообщений consumer забирает заранее |
| `MAX_RETRIES` | `3` | Число попыток перед отправкой в DLQ |
| `FAILURE_RATE` | `0.15` | Вероятность симулированного сбоя |
| `PROCESSING_MIN_DELAY` / `PROCESSING_MAX_DELAY` | `0.5` / `2.0` | Диапазон задержки обработки, сек |

```bash
docker compose run --rm -e PREFETCH_COUNT=1 -e FAILURE_RATE=0.5 consumer python consumer.py
```

Полный код — [`consumer/consumer.py`](consumer/consumer.py) и [`consumer/dlq_handler.py`](consumer/dlq_handler.py).

---

## Настройка мониторинга

Полная теория, разбор метрик по уровням (queue/channel/node) и готовые PromQL-запросы — в [`docs/monitoring.md`](docs/monitoring.md). Здесь — краткая шпаргалка.

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

Правила лежат в [`monitoring/alerts.yml`](monitoring/alerts.yml): рост очереди, отсутствие consumer-ов, нехватка диска, высокая память. Посмотреть активные алерты — `http://localhost:9090/alerts`. Подробности — [`docs/monitoring.md`](docs/monitoring.md#4-алертинг).

---

## Лабораторные кейсы

> Каждый кейс можно автоматически проверить: `./lab-solutions/case-N/verify.sh` (после выполнения шагов руками). Скрипты используют Management HTTP API и не заменяют ручное наблюдение в UI — только дополняют его.

### Кейс 1: Отправка и потребление сообщений (базовый поток)

**Цель:** Запустить producer и consumer, убедиться что сообщения доставляются.

**Шаги:**

1. Запустите RabbitMQ и убедитесь что он healthy:
```bash
docker compose up -d rabbitmq
docker compose ps
```

2. Отправьте 5 сообщений:
```bash
docker compose run --rm producer python producer.py --count 5 --delay 0.2
```

3. Проверьте в Management UI (`http://localhost:15672`) — вкладка **Queues** → очередь `orders` должна показать 5 сообщений (Ready).

4. Запустите consumer:
```bash
docker compose run --rm consumer python consumer.py
```

5. Наблюдайте в Management UI как сообщения уменьшаются: Ready: 0, Unacknowledged: 0.

**Ожидаемый результат:** Все 5 сообщений обработаны, очередь пуста.

**Автопроверка:** `./lab-solutions/case-1/verify.sh`

---

### Кейс 2: Влияние prefetch_count

**Цель:** Понять как `prefetch_count` влияет на распределение нагрузки.

**Шаги:**

1. Отправьте 20 сообщений:
```bash
docker compose run --rm producer python producer.py --count 20 --delay 0.1
```

2. Запустите consumer с `prefetch_count=1`:
```bash
docker compose run --rm -e PREFETCH_COUNT=1 consumer python consumer.py
```

3. Заметьте скорость обработки. Остановите consumer (Ctrl+C).

4. Повторите с `prefetch_count=10`:
```bash
docker compose run --rm producer python producer.py --count 20 --delay 0.1
docker compose run --rm -e PREFETCH_COUNT=10 consumer python consumer.py
```

5. Сравните:
   - **prefetch_count=1**: consumer обрабатывает по одному сообщению. Медленно, но безопасно.
   - **prefetch_count=10**: consumer берёт больше сообщений сразу. Быстрее, но выше нагрузка/риск при сбое.

**В Management UI:** вкладка **Channels** → `Unacked` — количество сообщений, взятых consumer-ом, но ещё не обработанных.

**Автопроверка:** `./lab-solutions/case-2/verify.sh 1` (пока запущен consumer с prefetch=1)

---

### Кейс 3: Dead Letter Queue — сбои и DLQ

**Цель:** Наблюдать как сообщения попадают в DLQ после `MAX_RETRIES` неудачных попыток.

**Шаги:**

1. Отправьте 30 сообщений:
```bash
docker compose run --rm producer python producer.py --count 30 --delay 0.1
```

2. Запустите consumer с высокой вероятностью сбоя:
```bash
docker compose run --rm -e FAILURE_RATE=0.5 -e MAX_RETRIES=3 consumer python consumer.py
```

3. Наблюдайте в Management UI: очередь `orders` уменьшается, `orders-dlq` растёт; вкладка **Exchanges → orders-dlx** — маршрутизация DLQ.

4. В логах consumer вы увидите:
```
[ERROR] Failed to process order abc12345...: payment_timeout
[WARNING] Message sent to DLQ: order_id=abc12345... (after 3 attempts)
```

**Ожидаемый результат:** Часть сообщений обработана успешно, часть попала в DLQ.

**Автопроверка:** `./lab-solutions/case-3/verify.sh`

---

### Кейс 4: Topic Exchange — маршрутизация по паттернам

**Цель:** Научиться маршрутизировать сообщения через topic exchange.

**Шаги:**

1. Отправьте 20 сообщений всех типов:
```bash
docker compose run --rm producer python producer.py --count 20 --type mixed --delay 0.2
```

2. В Management UI, вкладка **Queues**:
   - `orders-topic-urgent`: сообщения с паттерном `order.*.urgent` (express-заказы с priority ≥ 7)
   - `orders-topic-regular`: сообщения с паттерном `order.*.regular`

3. Отправьте только express:
```bash
docker compose run --rm producer python producer.py --count 10 --type express --delay 0.2
```

**Ожидаемый результат:** Все express-заказы попадают в `orders-topic-urgent`, остальные — в `orders-topic-regular`.

**Автопроверка:** `./lab-solutions/case-4/verify.sh`

---

### Кейс 5: Priority Queues — приоритетная обработка

**Цель:** Сравнить обработку сообщений с разными приоритетами внутри одной очереди.

> ⚠️ В очередь `orders` попадают **только** заказы типа `standard` (см. [`docs/concepts.md`](docs/concepts.md) — `express` уходит в `orders-topic`, `bulk` — в `orders-fanout`). А приоритет `standard`-заказов всегда одинаковый (5), если не задать `--priority` явно. Поэтому для этого кейса используется флаг `--priority`, форсирующий конкретный приоритет независимо от типа заказа.

**Шаги:**

1. Остановите/не запускайте consumer заранее — сначала нужно накопить сообщения с разными приоритетами в одной очереди.

2. Отправьте сообщения с низким и высоким приоритетом (оба — `--type standard`, чтобы оба оказались в `orders`):
```bash
docker compose run --rm producer python producer.py --count 5 --type standard --priority 3 --delay 0.1
docker compose run --rm producer python producer.py --count 5 --type standard --priority 9 --delay 0.1
```

3. В Management UI → **Queues → orders → Get messages** (Ack mode: `Nack requeue`, Messages: 10) — посмотрите порядок: сообщения с приоритетом 9 должны идти раньше сообщений с приоритетом 3, независимо от порядка публикации.

4. Запустите consumer и убедитесь, что он тоже забирает высокоприоритетные сообщения первыми:
```bash
docker compose run --rm consumer python consumer.py
```

**Ожидаемый результат:** Сообщения с приоритетом 9 обрабатываются раньше сообщений с приоритетом 3.

**Автопроверка:** `./lab-solutions/case-5/verify.sh` (проверяет конфигурацию `x-max-priority`, порядок обработки — только визуально)

---

### Кейс 6: Масштабирование consumers — балансировка нагрузки

**Цель:** Наблюдать round-robin балансировку при нескольких consumer-ах.

**Шаги:**

1. Отправьте 30 сообщений:
```bash
docker compose run --rm producer python producer.py --count 30 --delay 0.1
```

2. Запустите 3 consumer-а в разных терминалах:
```bash
docker compose run --rm --name consumer-1 consumer python consumer.py
docker compose run --rm --name consumer-2 consumer python consumer.py
docker compose run --rm --name consumer-3 consumer python consumer.py
```

3. В Management UI → **Queues → orders → Consumers**: 3 потребителя, сообщения распределяются между ними.

4. Остановите один consumer — оставшиеся 2 забирают нагрузку.

**Ожидаемый результат:** При 3 consumer-ах обработка в ~3 раза быстрее; при остановке одного — автоматическая ребалансировка.

**Автопроверка:** `./lab-solutions/case-6/verify.sh` (пока запущено ≥ 2 consumer-ов)

---

### Кейс 7: Memory/Disk Alarms — блокировка producer

**Цель:** Наблюдать как RabbitMQ блокирует producer при нехватке ресурсов.

**Шаги:**

1. В `rabbitmq.conf` **замените** (не добавьте!) строку `vm_memory_high_watermark.relative = 0.4` на:
```ini
vm_memory_high_watermark.absolute = 200MB
```

2. Перезапустите RabbitMQ:
```bash
docker compose restart rabbitmq
```

3. Сгенерируйте нагрузку (требует `pip install -r producer/requirements.txt` локально, либо запустите изнутри контейнера `producer`):
```bash
python scripts/load-test.py --total 200000 --workers 10 --size 2048
```

4. В Management UI → **Overview** появляется предупреждение **Memory Alarm**; producer блокируется (`connection.blocked`).

5. Восстановите исходный параметр в `rabbitmq.conf`:
```ini
vm_memory_high_watermark.relative = 0.4
```
```bash
docker compose restart rabbitmq
```

**Ожидаемый результат:** При превышении лимита памяти producer блокируется; после восстановления лимита — работает нормально.

**Автопроверка:** `./lab-solutions/case-7/verify.sh`

---

### Кейс 8: Симуляция кластера — 2 узла с quorum queues

**Цель:** Настроить 2-узловой кластер и использовать quorum queues.

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

5. Создайте quorum queue через `rabbitmqadmin` (входит в образ `*-management`):
```bash
docker exec -it rabbitmq-1 rabbitmqadmin declare queue name=orders-quorum durable=true \
  arguments='{"x-queue-type": "quorum"}'
```

6. Проверьте в Management UI: **Overview** → кластер из 2 узлов; **Queues → orders-quorum** с type `quorum`.

**Ожидаемый результат:** 2-узловой кластер работает, quorum queue реплицирует сообщения между узлами.

**Автопроверка:** `./lab-solutions/case-8/verify.sh`

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

Подробности — [`docs/clustering.md`](docs/clustering.md#5-partition-handling--что-происходит-при-split-пары-узлов).

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

### Бонус 1: TLS для RabbitMQ соединений

**Цель:** Настроить шифрование соединений между producer/consumer и RabbitMQ.

**Шаги:**

1. Сгенерируйте сертификаты одной командой (обёртка над `openssl`):
```bash
./scripts/generate-tls-certs.sh
```
Файлы появятся в `tls/` (директория в `.gitignore`, секреты никогда не коммитятся).

2. Добавьте в `rabbitmq.conf`:
```ini
listeners.ssl.default = 5671
ssl_options.cacertfile = /etc/rabbitmq/ssl/ca.pem
ssl_options.certfile = /etc/rabbitmq/ssl/server.pem
ssl_options.keyfile = /etc/rabbitmq/ssl/server.key
ssl_options.verify = verify_peer
ssl_options.fail_if_no_peer_cert = true
```
и смонтируйте `./tls` как `/etc/rabbitmq/ssl` в `docker-compose.yml`.

3. Подключитесь с TLS:
```python
import ssl
import pika

context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
context.load_verify_locations("tls/ca.pem")
context.load_cert_chain("tls/client.pem", "tls/client.key")

params = pika.ConnectionParameters(
    host="localhost",
    port=5671,
    ssl_options=pika.SSLOptions(context),
)
```

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

| Практика | Почему важно |
|----------|-------------|
| Минимум 3 узла в кластере | Quorum Queues требуют большинства (2 из 3, 3 из 5). На 2 узлах при partition теряется доступность |
| Quorum Queues вместо Classic queues | Raft-консенсус, устойчивость к partition. Classic mirrored queues удалены в RabbitMQ 4.x |
| Нечётное количество узлов | Предотвращает split-brain: при 3 узлах — 2 здоровых из 3 = кластер жив |
| Узлы в разных AZ/зонах | Выживание при аварии одной зоны |
| Отдельные диски для Mnesia и msg_store | Снижает contention между метаданными и данными сообщений |

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

| Практика | Реализация |
|----------|-----------|
| Включи TLS | `listeners.ssl.default = 5671` + сертификаты (см. Бонус 1) |
| Удали guest-аккаунт | `rabbitmqctl delete_user guest` |
| Отдельный user на сервис | Не используй одного user для всех producers/consumers |
| ACL | Ограничь доступ к очередям по vhost и routing key |
| Firewall на management-порт | 15672 — только для admin-подсети |
| Vhost на каждый env | dev/staging/prod — разные vhost-ы (см. `scripts/create-vhost.sh`) |

```bash
rabbitmqctl add_user app_producer "$(openssl rand -base64 24)"
rabbitmqctl set_permissions -p /production app_producer ".*" "" ""   # только publish

rabbitmqctl add_user app_consumer "$(openssl rand -base64 24)"
rabbitmqctl set_permissions -p /production app_consumer "" ".*" ""   # только consume
```

#### Мониторинг и алерты

| Метрика | Порог алерта |
|---------|-------------|
| `rabbitmq_queue_messages` | > 10 000 или растёт быстрее 1000/5 мин |
| `rabbitmq_queue_consumers` | = 0 дольше 2 минут |
| `rabbitmq_process_resident_memory_bytes` | > 80% от `rabbitmq_resident_memory_limit_bytes` |
| `rabbitmq_disk_space_available_bytes` | < 2GB |
| `rabbitmq_connections` | Резкий скачок (> 2x от нормы) |

Готовые правила — [`monitoring/alerts.yml`](monitoring/alerts.yml), теория — [`docs/monitoring.md`](docs/monitoring.md).

#### Backup и Recovery

| Практика | Как делать |
|----------|-----------|
| Бэкап definitions | `rabbitmqadmin export /backup/definitions-$(date +%Y%m%d).json` ежедневно через cron |
| Бэкап Mnesia | `rabbitmqctl stop_app && rsync /var/lib/rabbitmq/mnesia/ /backup/mnesia/ && rabbitmqctl start_app` |
| Бэкап Erlang Cookie | Без cookie не восстановить кластер — храните в Vault/Secrets Manager |
| Runbook | Документируйте: add node, remove node, recover from partition, recover from disk failure |

---

### ❌ Чего НЕЛЬЗЯ делать

#### Антипаттерны конфигурации

| ❌ Антипаттерн | Почему плохо | ✅ Правильно |
|----------------|----------------|-------------|
| `prefetch_count = 0` (безлимитный) | Один consumer забирает всё, риск OOM | `prefetch_count = 5-50` |
| `auto_ack = true` в production | Потеря сообщений при crash consumer-а | `auto_ack = false` + явный `basic_ack()` |
| Classic Mirror Queues | Удалены в RabbitMQ 4.x, split-brain risk | Quorum Queues |
| Статический `peer_discovery_backend = classic_config` | Нужно менять конфиг при каждом узле | K8s API / Consul / DNS discovery |
| Нет `x-dead-letter-exchange` | «Битые» сообщения циркулируют бесконечно | DLX + DLQ на каждый сервис |
| Один exchange для всех сервисов | Маршрутизация неуправляема | Изолированные exchanges per service |
| Нет message TTL | Очередь растёт бесконечно при падении consumer-а | `x-message-ttl` или policy |

#### Антипаттерны эксплуатации

| ❌ Антипаттерн | Что происходит | ✅ Правильно |
|----------------|------------------|-------------|
| `rabbitmqctl force_boot` без понимания | Риск потери данных при partition | Сначала `cluster_status`, force_boot — последняя мера |
| Ручное перемещение очередей между узлами | Нарушает балансировку | `queue_master_locator = min-masters` |
| Удаление узла без `forget_cluster_node` | «Призрачный» узел в кластере | `rabbitmqctl forget_cluster_node <node>` перед выключением |
| Игнорирование memory alarms | Producer-ы блокируются | Alerts + автоскейлинг |
| Диск с < 5GB свободного места | Disk alarm → блокировка → outage | Мониторинг + алерт при < 10GB |
| Один vhost для всех окружений | Dev может повлиять на prod | Отдельный vhost на env |

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

- [`docs/concepts.md`](docs/concepts.md) — теория: exchanges, queues, bindings, routing key, DLX
- [`docs/clustering.md`](docs/clustering.md) — теория: кластеризация, Raft, quorum queues, partition handling
- [`docs/monitoring.md`](docs/monitoring.md) — теория: метрики по уровням (queue/channel/node), PromQL, алерты

---

## Что было исправлено в этой версии

Черновик руководства содержал несколько неточностей, которые исправлены в этом репозитории:

1. В `docker-compose.yml` отсутствовал сервис `node-exporter`, хотя `prometheus.yml` его скрейпил, а он заявлен в стеке технологий — добавлен.
2. Формат `rabbitmq.json` для Grafana был в виде обёртки API-импорта дашбордов (`{"dashboard": ..., "overwrite": true}`), а не «сырой» модели, читаемой file-provisioning механизмом — переписан.
3. Часть PromQL-запросов ссылалась на несуществующие метрики: `rabbitmq_queue_messages_unacknowledged` (правильно — `rabbitmq_queue_messages_unacked`), `rabbitmq_queue_messages_delivered_total`/`_acknowledged_total` на уровне очереди (существуют только на уровне канала, без `queue`-лейбла, и без "acknowledged"), `node_filesystem_avail_bytes` с недостижимым для named-volume mountpoint (заменено на нативную `rabbitmq_disk_space_available_bytes`).
4. `dlq_handler` был функцией внутри `consumer.py`, хотя `Dockerfile` и структура репозитория предполагали отдельный файл `dlq_handler.py` — вынесен в отдельный модуль.
5. Лог DLQ-обработчика подставлял `_dlq_timestamp` в текст про причину сбоя (баг копипасты) — исправлено.
6. Аргументы очереди `orders` при объявлении в `producer.py`/`consumer.py` не совпадали с `definitions.json` (отсутствовали `x-dead-letter-exchange`/`x-dead-letter-routing-key`), что могло приводить к `406 PRECONDITION_FAILED` — выровнены через общую константу.
7. `rabbitmq.conf` и `definitions.json` не были отражены в дереве структуры репозитория, хотя монтировались в `docker-compose.yml` — добавлены.
8. `.env` был заявлен, но не использовался — `docker-compose.yml` теперь читает креды/версии/порты из `.env`. При этом сам `.env` — антипаттерн для коммита в git (даже с дефолтными кредами): в репозиторий уходит только шаблон [`.env.example`](.env.example), а `.env` — в `.gitignore` и создаётся локально через `cp .env.example .env`.
9. `monitoring/grafana/grafana.ini` монтировался как файл без описанного содержимого — добавлен минимальный валидный конфиг.
10. В Кейсе 7 предлагалось добавить `vm_memory_high_watermark.absolute`, конфликтующий с уже заданным `vm_memory_high_watermark.relative` — в инструкции явно указано, что один параметр должен заменить другой.
11. Правила алертинга из раздела Production не были подключены ни к одному файлу — вынесены в `monitoring/alerts.yml` и подключены через `rule_files`.
12. `producer/Dockerfile` и `consumer/Dockerfile` задавали `ENTRYPOINT ["python", "producer.py"]`/`["python", "consumer.py"]`, при этом все команды быстрого старта и лабораторных кейсов вызывают `docker compose run --rm producer python producer.py ...` — при наличии `ENTRYPOINT` это приводило к двойному запуску скрипта (`python producer.py python producer.py --count 10 ...`) и ошибке `unrecognized arguments`. `ENTRYPOINT` убран — команда теперь полностью задаётся вызывающей стороной.
13. `definitions.json` не содержал секции `vhosts`/`users`/`permissions`. Начиная с некоторой версии RabbitMQ, если задан `management.load_definitions`, брокер **пропускает** штатный шаг создания дефолтного vhost `/` и пользователя `guest`/`guest`, ожидая, что они будут описаны в самом файле определений — без этого брокер не проходил boot с ошибкой `Please create virtual host "/" prior to importing definitions.`. Добавлены `vhosts`/`users` (с валидным `password_hash` для `guest`/`guest`, по алгоритму `rabbit_password_hashing_sha256`) и `permissions`.
14. `producer.py` вызывал `channel.confirm_delivery()` внутри `publish_message()`, то есть при каждой отправке — pika разрешает включать confirm-режим только один раз на канал, поэтому начиная со второго сообщения в лог летела ошибка `confirm_delivery: confirmation was already enabled on channel=1`. Включение confirms перенесено в `main()`, один раз на канал.
15. **Ключевой логический баг:** `get_routing_key()` в `producer.py` всегда возвращала `order.{type}.{urgent|regular}` независимо от того, в какой exchange публикуется сообщение. Для `orders-direct` в `definitions.json` объявлен binding с фиксированным routing key `order.new` — несовпадающий routing key делал `standard`-заказы **unroutable** (сообщение молча отбрасывалось брокером, publisher confirm при этом всё равно приходил успешным, так как `mandatory` не задан). На практике это означало, что очередь `orders` никогда не получала сообщений вообще. Routing key теперь вычисляется в зависимости от целевого exchange (`order.new` для `orders-direct`, паттерн `order.{type}.{urgent|regular}` только для `orders-topic`).
16. Из-за прошлого пункта и того, что приоритет однозначно определяется типом заказа (`PRIORITY_MAP`), у Кейса 5 (приоритетные очереди) не было реального сценария: в `orders` попадают только `standard`-заказы, а у них всегда одинаковый приоритет — сравнивать было нечего. Добавлен флаг `--priority` в `producer.py`, форсирующий приоритет независимо от типа, и обновлены шаги Кейса 5.
17. **Ключевой логический баг в `consumer.py`:** retry-логика опиралась на заголовок `x-retry-count`, который должен был инкрементироваться и «доезжать» до следующей попытки через `basic_nack(requeue=True)`. Но `nack` с `requeue=True` возвращает сообщение в очередь с **неизменными** исходными properties — заголовок никогда фактически не увеличивался, и сообщение бесконечно кружило между очередью и consumer-ом, никогда не достигая `MAX_RETRIES` и DLQ (в логах это выглядело как вечные `Retrying (1/3)` для одного и того же `order_id`). Исправлено на явный `ack` исходного сообщения + `basic_publish` в ту же очередь (через nameless-exchange, routing key = имя очереди) с обновлённым заголовком — так retry-счётчик реально сохраняется между попытками.
18. Healthcheck RabbitMQ в `docker-compose.yml`/`docker-compose.cluster.yml` имел слишком короткий бюджет ожидания (`start_period: 30s` + `retries: 5` × `interval: 15s` ≈ 105с). На практике первый старт (миграции feature flags, импорт `definitions.json`) может занимать заметно дольше — в одном из тестовых прогонов заняло 145с — из-за чего Docker помечал полностью исправный, но ещё стартующий контейнер как `unhealthy`, и зависимые сервисы не поднимались (`dependency failed to start`). Увеличены `start_period` до 90s и `retries` до 10 (бюджет ≈ 240с); `scripts/setup.sh` синхронизирован (таймаут поднят до 240с).

Все перечисленные проблемы обнаружены и проверены практически: сервисы поднимались через `docker compose up`, было пройдено сквозное подтверждение работы producer → RabbitMQ → consumer → DLQ → Prometheus → Grafana.

---

> **Актуально на июль 2026**
> Руководство подготовлено для RabbitMQ 4.1 (management) с использованием Docker Compose v2, Python 3.12, Prometheus v2.54.1, Node Exporter v1.12.1 и Grafana 11.1.0.
