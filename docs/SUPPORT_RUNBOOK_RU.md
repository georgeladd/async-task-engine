# Регламент технической поддержки и устранения аварий (Runbook)

🌐 **[English](SUPPORT_RUNBOOK.md)** • **[Русский](SUPPORT_RUNBOOK_RU.md)**

Добро пожаловать в операционный регламент для **Async Task Engine**. Документ предназначен для инженеров технической поддержки L2/L3, дежурных администраторов и SRE для быстрой диагностики сбоев, понимания поведения системы и снижения времени восстановления (MTTR)

---

## 1. Быстрый ввод в систему (Карта сервисов за 5 минут)

### 1.1. Ключевые компоненты и точки доступа
| Компонент | Порт | Роль в системе | Интерфейс / Доступ |
|---|---|---|---|
| **Веб-консоль управления** | `8000` | Первичная диагностика L2: графики, снятие локов в 1 клик, Replay из DLQ | [http://localhost:8000/dashboard](http://localhost:8000/dashboard) |
| **FastAPI Producer** | `8000` | Прием задач, генерация UUID, запись статуса в Redis | [http://localhost:8000/docs](http://localhost:8000/docs) |
| **Метрики Prometheus** | `8000` | Скрейпинг метрик в реальном времени для Grafana | [http://localhost:8000/metrics](http://localhost:8000/metrics) |
| **RabbitMQ** | `5672` / `15672` | Direct exchange, основная очередь и Dead-Letter Queue | [http://localhost:15672](http://localhost:15672) (`guest` / `guest`) |
| **Redis** | `6379` | Статусы задач (`task:status:*`) и блокировки (`lock:resource:*`) | Доступ через `redis-cli`, CLI или веб-консоль |
| **Worker** | Фоновый | Потребление задач, захват блокировок, потоковая обработка | Логи контейнера `async-engine-worker` |
| **Консольная утилита (CLI)** | Терминал | Экспресс-диагностика, ручной запуск и управление локами | `python -m src.cli --help` |

### 1.2. Базовые инварианты системы
- **Жизненный цикл задачи:** `PENDING` -> `RUNNING` -> `COMPLETED` (или `FAILED` / `DEAD_LETTERED`)
- **Блокировка ресурсов:** Только **один** воркер может обрабатывать задачи для конкретного `resource_id` одновременно. Остальные задачи с этим же ресурсом будут отложены обратно в очередь до освобождения или экспирации блокировки
- **Dead-Letter Queue:** Если задача упала с ошибкой больше `MAX_TASK_RETRIES` раз (по умолчанию: 3), она выводится из основного потока в очередь `tasks_dead_letter`

---

## 2. Экстренный 30-секундный чеклист первичной диагностики

Когда приходит тикет *"Задачи зависли"* или *"Данные не обновились"*, выполни диагностику по шагам:

```bash
# 0. Открой веб-консоль в браузере для моментальной визуальной оценки:
# http://localhost:8000/dashboard (Проверь индикаторы здоровья, очередь, блокировки и DLQ)

# 1. Проверь доступность API сервиса через CLI
python -m src.cli health

# 2. Проверь наличие зависших блокировок в Redis
python -m src.cli locks

# 3. Проверь статус контейнеров Docker
docker-compose ps

# 4. Проверь очереди и наличие активных потребителей в RabbitMQ
docker exec async-engine-rabbitmq rabbitmqctl list_queues name messages messages_unacknowledged consumers

# 5. Проверь последние ошибки в логах воркера
docker logs --tail 100 async-engine-worker
```

---

## 3. Матрица типовых инцидентов и регламенты решения

---

### Инцидент A: Задачи накапливаются в статусе `PENDING` (Рост очереди)

#### Симптомы
- Запросы принимаются с кодом 202, но вызов `GET /api/v1/tasks/{task_id}` возвращает `pending` в течение нескольких минут
- В RabbitMQ в очереди `tasks_primary` растет счетчик `messages`, а `consumers = 0`

#### Причины
1. Контейнер воркера упал или был остановлен
2. Наплыв входящих задач превысил пропускную способность одного воркера

#### Диагностика
```bash
# Проверка статуса контейнера воркера
docker-compose ps worker

# Просмотр причины падения воркера
docker logs --tail 50 async-engine-worker
```

#### Регламент устранения
1. Если контейнер воркера упал, перезапусти его:
   ```bash
   docker-compose restart worker
   ```
2. При резком росте нагрузки увеличь количество воркеров горизонтально:
   ```bash
   docker-compose up -d --scale worker=4 --no-recreate
   ```
3. Убедись, что новые воркеры подключились к очереди:
   ```bash
   docker exec async-engine-rabbitmq rabbitmqctl list_queues name consumers
   ```

---

### Инцидент B: Циклические предупреждения "Resource is locked"

#### Симптомы
- В логах воркера непрерывно сыплются сообщения:
  `WARNING: Resource 'customer_tenant_42' is locked by another task. Requeuing task ...`
- Задачи по конкретному клиенту или ресурсу стоят на месте, в то время как остальные обрабатываются в штатном режиме

#### Причины
Предыдущая задача над этим `resource_id` аварийно завершилась без исполнения блока `finally: await lock.release()`, оставив "осиротевший" ключ в Redis

#### Диагностика
```bash
# Проверка оставшегося времени жизни (TTL) блокировки
docker exec async-engine-redis redis-cli ttl "lock:resource:<resource_id>"

# Просмотр токена владельца блокировки
docker exec async-engine-redis redis-cli get "lock:resource:<resource_id>"
```

#### Регламент устранения
1. Убедись по логам воркера, что над ресурсом прямо сейчас не идет реальная обработка:
   ```bash
   docker logs async-engine-worker | grep "<resource_id>"
   ```
2. Если активной обработки нет и лок завис, сними блокировку одним из способов:
   - **Через веб-консоль (Рекомендуется):** Открой [http://localhost:8000/dashboard](http://localhost:8000/dashboard), найди ресурс в таблице активных локов и нажми **"Force Unlock"**
   - **Через консольную утилиту:** `python -m src.cli unlock "<resource_id>"`
   - **Напрямую через Redis CLI:** `docker exec async-engine-redis redis-cli del "lock:resource:<resource_id>"`
3. Воркер автоматически подхватит задачу и начнет выполнение на следующем проходе очереди

---

### Инцидент C: Сообщения попадают в Dead-Letter Queue (`tasks_dead_letter`)

#### Симптомы
- В очереди RabbitMQ `tasks_dead_letter` появились сообщения
- Запрос статуса задачи в API возвращает `"status": "dead_lettered"`
- В веб-консоли горит красный бейдж на карточке Dead-Letter Queue

#### Причины
Задача столкнулась с неустранимой бизнес-ошибкой (например, целевая база данных отклонила структуру батча, внешнее API недоступно) и исчерпала лимит из 3 попыток

#### Регламент устранения и быстрое восстановление
- **Повторный запуск в 1 клик (Replay):** Открой [http://localhost:8000/dashboard](http://localhost:8000/dashboard), посмотри причину сбоя в таблице DLQ и нажми **"Replay"** для возврата задачи в основную очередь
- **Эскалация разработчикам:** Если для исправления требуется правка кода, нажми **"Escalate"** для автоматической генерации отчета об аварии (Incident Dossier) со стектрейсом и параметрами вызова

#### Диагностика
```bash
# Просмотр количества сообщений в очереди сбоев
docker exec async-engine-rabbitmq rabbitmqctl list_queues name messages | grep dead_letter

# Поиск стека ошибки в логах воркера
docker logs async-engine-worker | grep "Routing to Dead-Letter Queue" -B 5 -A 2
```

#### Дополнительные действия
1. Выясни корневую причину по логу (например, отсутствие колонки в целевой таблице)
2. После устранения внешней проблемы:
   - Переотправь задачу через веб-консоль кнопкой Replay (или через `POST /api/v1/tasks`)
   - Если в DLQ скопились поврежденные тестовые сообщения, очисти очередь:
     ```bash
     docker exec async-engine-rabbitmq rabbitmqctl purge_queue tasks_dead_letter
     ```

---

### Инцидент D: Воркер завершился с кодом ошибки 137 (OOMKilled)

#### Симптомы
- Docker сообщает `async-engine-worker exited with code 137`
- Падение происходит в момент обработки очень крупных пакетов данных

#### Причины
Переполнение памяти из-за слишком большого значения `BATCH_CHUNK_SIZE` в сочетании с тяжелыми записями

#### Диагностика
```bash
# Мониторинг потребления памяти контейнером
docker stats --no-stream async-engine-worker
```

#### Регламент устранения
1. Уменьши размер чанка в файле конфигурации `.env`:
   ```bash
   # В файле .env:
   BATCH_CHUNK_SIZE=25
   ```
2. Примени обновленную конфигурацию к контейнеру:
   ```bash
   docker-compose up -d worker
   ```

### Инцидент E: Превышение лимитов сторонних API (HTTP 429) или исчерпание пула БД

#### Симптомы
- В логах воркера появляются ошибки `HTTP 429 Too Many Requests` от внешних систем
- База данных отклоняет новые коннекты из-за исчерпания connection pool

#### Причина
Слишком высокая скорость выборки и обработки чанков воркером, перегружающая внешние сервисы

#### Регламент устранения
1. Ограничь скорость обработки через Token Bucket в файле `.env`:
   ```bash
   # В файле .env:
   RATE_LIMIT_PER_SECOND=20.0
   ```
2. Перезапусти воркер без остановки других сервисов:
   ```bash
   docker-compose up -d worker
   ```

---

## 4. Полезная шпаргалка команд

### Запрос статуса задачи через curl
```bash
# Получение статуса и метрик выполнения в формате JSON
curl -s "http://localhost:8000/api/v1/tasks/<TASK_UUID>" | jq .
```

### Быстрые команды Redis
```bash
# Просмотр всех зарегистрированных ключей статусов задач
docker exec async-engine-redis redis-cli keys "task:status:*"

# Просмотр сырого JSON результата задачи
docker exec async-engine-redis redis-cli get "task:result:<TASK_UUID>"

# Просмотр активных токенов идемпотентности и привязанных UUID задач
docker exec async-engine-redis redis-cli keys "idempotency:*"
docker exec async-engine-redis redis-cli get "idempotency:<TOKEN>"
```

### Диагностика RabbitMQ
```bash
# Просмотр неподтвержденных (in-flight) сообщений в обработке воркерами
docker exec async-engine-rabbitmq rabbitmqctl list_queues name messages_unacknowledged
```

### Фильтрация структурированных JSON-логов (ELK / jq)
```bash
# Фильтрация потоковых логов по конкретному correlation_id задачи
docker logs -f async-engine-worker | jq -R 'fromjson? | select(.correlation_id == "<TASK_UUID>")'

# Вывод последних 200 ошибок с полным стектрейсом исключений
docker logs --tail 200 async-engine-worker | jq -R 'fromjson? | select(.level == "ERROR" or .level == "CRITICAL")'

# Просмотр попыток отправки и статусов вебхуков
docker logs async-engine-worker | jq -R 'fromjson? | select(.message | contains("Webhook"))'
```

### Правила алертинга Prometheus (Grafana / Alertmanager)
| Имя алерта | PromQL выражение | Критичность | Немедленное действие |
|---|---|---|---|
| **TasksInDeadLetterQueue** | `rate(dead_letter_tasks_total[5m]) > 0` | Warning | Проверить причину сбоя в DLQ через консоль или `rabbitmqctl` |
| **WorkerPoolStalled** | `active_worker_tasks == 0 and rabbitmq_queue_messages > 10` | Critical | Воркер упал при наличии очереди; перезапустить воркер |
| **HighTaskDurationP95** | `histogram_quantile(0.95, sum(rate(task_duration_seconds_bucket[5m])) by (le)) > 30` | Warning | Проверить нагрузку на базу данных или уменьшить `BATCH_CHUNK_SIZE` |

---

## 5. Правила эскалации (Когда привлекать разработчиков L3)

Передавай инцидент на третью линию разработки, если:
- RabbitMQ циклически падает с алертами нехватки диска или памяти (`rabbitmqctl status`)
- В Dead-Letter Queue попадают задачи с валидной схемой данных, что говорит об изменении контрактов внешних сервисов
- Зависание распределенных блокировок вызвано бесконечным циклом внутри бизнес-логики обработки
