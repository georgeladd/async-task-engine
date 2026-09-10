# Руководство разработчика по интеграции бизнес-логики (Developer Integration Guide)

🌐 **[English](DEVELOPER_GUIDE.md)** • **[Русский](DEVELOPER_GUIDE_RU.md)**

В этом руководстве описан практический процесс интеграции реальной бизнес-логики в **Async Task Engine**: от архитектурного контракта и жизненного цикла задачи до регистрации собственных обработчиков и работы с базами данных, внешними API и очередями

---

## 1. Сквозной жизненный цикл задачи

Движок спроектирован по принципу разделения ответственности (Separation of Concerns): транспортный уровень и очереди полностью изолированы от прикладного кода

```
[ Клиент / API ]
       │  POST /api/v1/tasks (с заголовком Idempotency-Key)
       ▼
[ FastAPI Producer ] ──(SET NX)──► [ Redis Кэш ] (Проверка дедупликации)
       │
       ▼ (AMQP Publish)
[ RabbitMQ Exchange ]
       │
       ▼ (Очередь tasks_primary)
[ Worker Consumer ] ──(SET NX)──► [ Redis Lock ] (Блокировка resource_id)
       │
       ▼
 [ Чанкер / Rate Limiter ] ──► Вызов @task_handler(chunk, params)
       │
       ├─► Успех: Redis COMPLETED + ACK + исходящий Webhook
       └─► Ошибка: Счетчик попыток -> Retry (nack) либо DLQ (reject)
```

1. **Прием задачи:** Внешний сервис отправляет JSON-запрос в `POST /api/v1/tasks`
2. **Атомарная дедупликация:** Redis фиксирует `Idempotency-Key` через команду `SET NX`. При повторном клике продюсер сразу возвращает сохраненный `task_id` с флагом `is_duplicate: true`, не нагружая брокер
3. **Сохранение контекста:** Исходное тело задачи сохраняется в `task:data:{task_id}` с TTL 7 дней (это позволяет восстановить payload при ручном Replay из консоли)
4. **Маршрутизация в воркер:** Сообщение падает в RabbitMQ с заданным приоритетом
5. **Изоляция ресурса:** Воркер захватывает распределенный Redis-лок на `resource_id`. Если ресурс занят, воркер делает паузу (backoff) и возвращает задачу в очередь
6. **Потоковая обработка:** Генератор `chunk_iterator` нарезает массив `payload.items` на чанки заданного размера и передает их в зарегистрированный обработчик под контролем Token Bucket
7. **Финализация:** При успехе воркер подтверждает сообщение (ACK), сохраняет результат и отправляет HMAC-подписанный вебхук на `callback_url`

---

## 2. Архитектура подключения обработчиков (Паттерн Registry)

Для соблюдения принципа открытости/закрытости (Open-Closed Principle из SOLID) движок использует реестр стратегий. Воркер не содержит жестких конструкций `if/elif/else` или `match/case` для каждого типа задач

### Создание реестра обработчиков

Создайте файл `src/handlers.py` (или пакет `src/handlers/`):

```python
"""Application business logic handlers registry."""

from collections.abc import Callable, Coroutine
from typing import Any

# Сигнатура функции-обработчика: принимает порцию записей и параметры задачи
TaskHandler = Callable[[list[dict[str, Any]], dict[str, Any]], Coroutine[Any, Any, None]]

HANDLERS: dict[str, TaskHandler] = {}


def task_handler(task_type: str):
    """Декоратор для автоматической регистрации обработчика в реестре движка."""
    def decorator(func: TaskHandler) -> TaskHandler:
        HANDLERS[task_type] = func
        return func
    return decorator


def get_handler(task_type: str) -> TaskHandler:
    """Возвращает зарегистрированный обработчик или выбрасывает исключение."""
    handler = HANDLERS.get(task_type)
    if not handler:
        raise ValueError(f"No handler registered for task_type '{task_type}'")
    return handler
```

---

## 3. Практические примеры реализации

### Сценарий А: Пакетный Bulk-Upsert в PostgreSQL

**Задача:** Загрузка 50 000 складских позиций без блокировки всей таблицы и без дефицита пула подключений

```python
# src/handlers/inventory.py
import asyncpg
from typing import Any
from src.handlers import task_handler

# Пул подключений инициализируется один раз при старте приложения
db_pool: asyncpg.Pool | None = None


@task_handler("inventory_sync")
async def handle_inventory_sync(
    chunk: list[dict[str, Any]],
    parameters: dict[str, Any],
) -> None:
    """Пакетное обновление товарных остатков в реляционной БД."""
    global db_pool
    if not db_pool:
        db_pool = await asyncpg.create_pool(
            dsn="postgresql://app_user:secret@postgres:5432/app_db",
            min_size=2,
            max_size=10,
        )

    # Формируем кортежи данных для пакетной вставки
    records = [
        (item["sku"], item["warehouse_id"], item["quantity"], item["price"])
        for item in chunk
    ]

    upsert_query = """
        INSERT INTO inventory_stocks (sku, warehouse_id, quantity, price, updated_at)
        VALUES ($1, $2, $3, $4, NOW())
        ON CONFLICT (sku, warehouse_id) DO UPDATE
        SET quantity = EXCLUDED.quantity,
            price = EXCLUDED.price,
            updated_at = NOW();
    """

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(upsert_query, records)
```

---

### Сценарий Б: Пакетная отправка вызовов во внешний API с троттлингом

**Задача:** Передача 10 000 платежных транзакций в шлюз эквайринга, где действует жесткий лимит 50 RPS (запросов в секунду)

```python
# src/handlers/payments.py
import httpx
from typing import Any
from src.handlers import task_handler


@task_handler("payment_gateway_sync")
async def handle_payment_sync(
    chunk: list[dict[str, Any]],
    parameters: dict[str, Any],
) -> None:
    """Пакетная отправка транзакций во внешний шлюз."""
    gateway_url = parameters.get("gateway_url", "https://api.payments.internal/v2/batch")

    payload = {
        "batch_id": parameters.get("batch_reference"),
        "transactions": chunk,
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(
            gateway_url,
            json=payload,
            headers={"Authorization": "Bearer secure-gateway-key"},
        )

        # 429 Too Many Requests: выбрасываем ошибку для срабатывания ретрая
        if response.status_code == 429:
            raise ConnectionError("Gateway rate limit exceeded, requesting task backoff")

        if not response.is_success:
            raise RuntimeError(f"Payment gateway rejected batch: HTTP {response.status_code}")
```

---

### Сценарий В: Генерация отчета со стримингом прогресса в Redis

**Задача:** Длительная выгрузка клиентских данных с отображением процента выполнения на фронтенде оператора

```python
# src/handlers/reporting.py
import json
import redis.asyncio as aioredis
from typing import Any
from src.handlers import task_handler


@task_handler("customer_report_export")
async def handle_report_export(
    chunk: list[dict[str, Any]],
    parameters: dict[str, Any],
) -> None:
    """Обработка выгрузки с обновлением прогресс-бара."""
    redis = aioredis.from_url("redis://localhost:6379/0", decode_responses=True)
    task_id = parameters.get("task_id")
    total_items = parameters.get("total_items", 1)

    try:
        # 1. Логика генерации строк отчета (например, форматирование CSV строки)
        for item in chunk:
            # Преобразование данных и запись во временный буфер
            pass

        # 2. Атомарное обновление прогресса задачи в Redis
        if task_id:
            processed = await redis.incrby(f"task:progress:count:{task_id}", len(chunk))
            percent = min(100, round((processed / total_items) * 100, 1))
            await redis.set(f"task:progress:percent:{task_id}", str(percent), ex=3600)
    finally:
        await redis.close()
```

---

## 4. Подключение реестра к ядру воркера

В файле `src/worker.py` внутри метода `process_task_payload` связывание занимает всего три строки:

```python
# Импортируем реестр
from src.handlers import get_handler

# Внутри TaskWorker.process_task_payload:
handler = get_handler(task.task_type)

for chunk in chunk_iterator(items, chunk_size):
    await self.rate_limiter.acquire()
    # Вызов зарегистрированной бизнес-логики
    await handler(chunk, task.payload.parameters)
    total_processed += len(chunk)
```

---

## 5. Динамическая маршрутизация очередей и специализированные воркеры

В реальном продакшне характер задач различается: легкие задачи синхронизации выполняются за 20 миллисекунд, а тяжелая генерация PDF-отчетов или обработка видео может занимать минуты и требовать гигабайты оперативной памяти. Если запускать их в общем потоке, возникает эффект **«шумных соседей» (Noisy Neighbors)**: тяжелые задачи забивают очередь, а быстрые клиентские запросы зависают

Для решения этой проблемы движок поддерживает **динамическую маршрутизацию на базе Topic Exchange**, сочетающую дефолтный пул и выделенные изолированные воркеры без перезапуска всей системы

```
[ Клиент / API ] ──► (Publish с routing_key="tasks.heavy.pdf_render")
                                  │
                      [ tasks_topic_exchange ]
                     ╱                        ╲
   (binding: tasks.general.*)            (binding: tasks.heavy.*)
                ▼                                      ▼
       [ Очередь: tasks_default ]             [ Очередь: tasks_heavy ]
                │                                      │
       [ Дефолтные воркеры ]                  [ Выделенные воркеры ]
    (легкие задачи: sync, CRM)              (тяжелые задачи: 4GB RAM, ML)
```

### 5.1. Дефолтный пул по умолчанию (Catch-All)

Пока у задачи нет специфических требований к ресурсам или изоляции, она направляется в общий дефолтный пул:
* Очередь `tasks_default` привязана к обменнику с маской `tasks.general.*` (или `#`)
* В ней работают стандартные воркеры, содержащие реестр `@task_handler` для 90-95% типовых прикладных операций
* Это гарантирует минимальную сложность архитектуры на старте: вам не нужно создавать отдельную очередь для каждого нового типа задач

### 5.2. Подключение выделенного воркера на лету (Zero-Downtime)

Когда в системе появляется ресурсоемкая задача (например, рендеринг PDF или ML-инференс), мы подключаем для нее изолированный пул воркеров без перезапуска API и других компонентов

#### Шаг 1. Отправка задачи с префиксом маршрутизации
В клиентском запросе указывается тип задачи с категорией (например, `heavy.pdf_render`):
```json
POST /api/v1/tasks
{
  "task_type": "heavy.pdf_render",
  "resource_id": "report_company_42",
  "payload": {
    "items": [{"document_id": "INV-2026-901"}],
    "parameters": {"format": "pdf", "dpi": 300}
  }
}
```
API автоматически формирует routing key: `tasks.heavy.pdf_render`

#### Шаг 2. Запуск выделенного воркера (Consumer-Driven Topology)
Новый воркер запускается с указанием собственной очереди и маски маршрутизации:

```python
# src/worker_heavy.py (или запуск стандартного воркера с флагами)
import asyncio
from src.worker import TaskWorker
from src.broker import RabbitMQBroker

async def run_heavy_worker():
    broker = RabbitMQBroker()
    await broker.connect()

    # Динамическое создание очереди и связывание по маске tasks.heavy.*
    channel = await broker.get_channel()
    queue = await channel.declare_queue("tasks_heavy", durable=True)
    exchange = await channel.declare_exchange("tasks_exchange", type="topic", durable=True)
    await queue.bind(exchange, routing_key="tasks.heavy.*")

    worker = TaskWorker(broker=broker, queue_name="tasks_heavy")
    await worker.start()

if __name__ == "__main__":
    asyncio.run(run_heavy_worker())
```

#### Шаг 3. Изоляция в Docker Compose
Выделенному воркеру в `docker-compose.yml` назначаются повышенные аппаратные лимиты:

```yaml
  worker-heavy:
    build: .
    container_name: async-engine-worker-heavy
    command: ["python", "-m", "src.worker_heavy"]
    deploy:
      resources:
        limits:
          cpus: "2.0"
          memory: 4096M
    environment:
      - WORKER_QUEUE_NAME=tasks_heavy
      - ROUTING_KEY=tasks.heavy.*
```

### 5.3. Защита от потери сообщений (Alternate Exchange Fallback)

Если задача со специфическим ключом отправлена, но соответствующий выделенный воркер еще не развернут, в брокере настраивается резервный обменник (`Alternate Exchange`):
* Сообщение не отбрасывается и не удаляется
* Брокер автоматически перекладывает его в fallback-очередь дефолтного пула
* Дефолтный воркер сохраняет статус или ожидает запуска целевого обработчика

---

## 6. Контракт обработки ошибок и отказоустойчивости

Движок разделяет сбои на **устранимые (Transient Errors)** и **фатальные (Unrecoverable Errors)**:

| Тип сбоя | Примеры | Поведение движка |
|---|---|---|
| **Устранимый сбой** | Сетевой таймаут к БД, HTTP 429 / 503 от API, блокировка таблицы | Воркер перехватывает ошибку, увеличивает счетчик `task:attempts:{id}`, возвращает статус `PENDING` и делает `nack(requeue=True)` |
| **Фатальный сбой** | Невалидный JSON, нарушение схемы данных, удаленная учетная запись | После исчерпания лимита (`MAX_TASK_RETRIES=3`) задача переводится в `DEAD_LETTERED`, отвергается через `reject(requeue=False)` и попадает в DLQ |

### Как писать безопасный код в обработчике

```python
@task_handler("billing_charge")
async def handle_billing(chunk: list[dict[str, Any]], params: dict[str, Any]) -> None:
    for item in chunk:
        # Фатальная ошибка: невалидные данные клиента
        if not item.get("account_id"):
            raise ValueError(f"Corrupted record: missing account_id in item {item}")

        # Устранимая ошибка: временный сбой платежного терминала
        try:
            await call_terminal(item)
        except ConnectionResetError as net_err:
            # Исключение всплывет в воркер и вызовет повтор (retry)
            raise ConnectionError(f"Terminal unavailable: {net_err}") from net_err
```

---

## 7. Локальная разработка и отладка

### Запуск окружения через Docker Compose

```bash
docker-compose up -d rabbitmq redis
```

### Запуск воркера в локальном окружении

```bash
# Активация виртуального окружения
source .venv/bin/activate

# Запуск рабочего узла
python -m src.worker
```

### Постановка тестовой задачи через CLI

```bash
# Отправка задачи с 200 записями
python -m src.cli submit \
  --type inventory_sync \
  --resource warehouse_spb_1 \
  --priority high \
  --items 200
```

### Проверка статуса исполнения

```bash
python -m src.cli status <TASK_UUID>
```

### Написание юнит-теста для нового обработчика

```python
# tests/test_inventory_handler.py
import pytest
from unittest.mock import AsyncMock, patch
from src.handlers.inventory import handle_inventory_sync


@pytest.mark.asyncio
async def test_inventory_handler_success():
    chunk = [{"sku": "SKU-01", "warehouse_id": "WH-1", "quantity": 10, "price": 100.0}]
    params = {"source": "unit_test"}

    mock_conn = AsyncMock()
    mock_pool = AsyncMock()
    mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

    with patch("src.handlers.inventory.db_pool", mock_pool):
        await handle_inventory_sync(chunk, params)
        mock_conn.executemany.assert_called_once()
```
