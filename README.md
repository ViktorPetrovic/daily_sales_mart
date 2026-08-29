# ETL-витрина продаж.

> Проект ETL для ежедневной загрузки данных о продажах в витрину данных.

---

## Оглавление

- [Описание проекта](#описание-проекта)
- [Архитектура](#архитектура)
- [Технологии](#технологии)
- [Структура проекта](#структура-проекта)
- [Требования](#требования)
- [Установка и запуск](#установка-и-запуск)
- [Настройка](#настройка)
- [Запуск DAG](#запуск-dag)
- [Мониторинг и логи](#мониторинг-и-логи)
- [CI/CD](#cicd)
- [Устранение неполадок](#устранение-неполадок)


---

## Описание проекта

ETL-процесс автоматически загружает данные из CSV-файлов в PostgreSQL, выполняет проверку качества данных и заполняет витрину данных (`mart.daily_sales_mart`) для ежедневных отчетов по продажам.

**Основные задачи DAG:**
1.  Проверка наличия файлов `orders_YYYY-MM-DD.csv` и `customers.csv`
2.  Создание таблиц в схеме `staging` и `mart`
3.  Загрузка данных из CSV в таблицы `staging.orders_raw` и `staging.customers_raw`
4.  Проверка качества данных (NULL значения, отрицательные значения)
5.  Заполнение витрины `mart.daily_sales_mart`
6.  Вывод данных витрины в логи

> **Важно** Данные заполняются только за предыдущий день, если вы запустили за 2026-08-28 то данные заполняться за 2026-08-27, если присутствует файл с данными за этот день. При этом все остальные данные будут удалены из таблиц перед записью.

---

## Архитектура

```
┌─────────────────────────────────────────────────────────────────┐
│                    Apache Airflow 3.3.1 (Docker)                │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                  DAG: sales_mart_etl                    │    │
│  │                                                         │    │
│  │      check_files  ─→ create_tables ─→ load_orders   ─┐  │    │
│  │                                     load_customers  ─┘  │    │
│  │                                                      │  │    │
│  │                               quality_check  ←───────┘  │    │
│  │                                     │                   │    │
│  │                                     ↓                   │    │
│  │                              fill_mart                  │    │
│  │                                     │                   │    │
│  │                                     ↓                   │    │
│  │                                 show_mart               │    │
│  └─────────────────────────────────────────────────────────┘    │
│                               │                                 │
└───────────────────────────────┼─────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                     PostgreSQL 15 (Docker)                      │
│  ┌────────────────┐          ┌─────────────────────────────┐    │
│  │ staging        │          │ mart                        │    │
│  │                │          │                             │    │
│  │ orders_raw     │          │ daily_sales_mart            │    │
│  │ customers_raw  │          │ (витрина продаж)            │    │
│  └────────────────┘          └─────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
                                ▲
                                │
┌─────────────────────────────────────────────────────────────────┐
│                     Файловая система (Docker)                   │
│  ┌────────────────────────────────────────────────────────┐     │
│  │  /opt/airflow/data/                                    │     │
│  │  ├── customers.csv                                     │     │
│  │  └── orders_YYYY-MM-DD.csv                             │     │
│  │                                                        │     │
│  │  /opt/airflow/include/sql/                             │     │
│  │  ├── create_table.sql                                  │     │
│  │  └── transform_mart.sql                                │     │
│  └────────────────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────┘
```

---

## Технологии

| Компонент          | Версия |        Назначение         |
|--------------------|--------|---------------------------|
| **Apache Airflow** | 3.3.1  | Оркестрация ETL-процессов |
| **PostgreSQL**     | 15     | Хранилище данных          |
| **Pandas**         | 3.0.5  | Обработка CSV-файлов      |
| **Docker**         | 24+    | Контейнеризация           |  
| **Docker Compose** | 2.0+   | Оркестрация контейнеров   |
| **Python**         | 3.10+  | Язык программирования     |

---

## Структура проекта

```
airflow_project/
├── .github/
│   └── workflows/
│       └── ci-cd.yaml              # CI/CD пайплайн (GitHub Actions)
├── dags/
│   └── sales_mart_etl.py           # DAG-файл
├── data/
│    ├── customers.csv               # Данные клиентов
│    └── orders_YYYY-MM-DD.csv       # Данные заказов  
├── include/    
│   └── sql/
│       ├── create_table.sql        # Создание таблиц
│       └── transform_mart.sql      # Заполнение витрины
├── logs/                           # Логи Airflow
├── plugins/                        # Плагины Airflow
├── Dockerfile                      # Docker-образ Airflow
├── requirements.txt                # Python-зависимости
├── docker-compose.yml              # Конфигурация Docker Compose
├── .env.example                    # Пример переменных окружения
├── .env                            # Переменные окружения (создать)
├── .gitignore                      # Игнорируемые файлы
└── README.md                       # Документация проекта
```

---

##  Требования

### Локальный компьютер:

-  **Docker** 24.0+
-  **Docker Compose** 2.0+
-  **Git**
-  **Порт 8080** (Airflow UI)
-  **Порт 5432** (PostgreSQL)

---

##  Установка и запуск

### 1. Клонировать репозиторий

```bash
git clone https://github.com/ViktorPetrovic/airflow_project.git
cd airflow_project
```

### 2. Создать файл `.env`

```bash
# Скопировать пример конфигурации
cp .env.example .env

# Отредактировать .env (если нужно)
nano .env  # или откройте в любом редакторе
```

### 3. Запустить Docker контейнеры

```bash
# Собрать образ и запустить контейнеры
docker-compose up -d --build

# Проверить, что все работает
docker-compose ps
```

### 4. Открыть Airflow UI

`http://localhost:8080`

> **Важно:** Пароль от доступа в Airflow UI для admin генерируется случайно при первом запуске.  

- **Логин:** `admin`
- **Пароль:** `сгенерируется автоматически. Чтобы узнать его в терминале Docker введите:`

```bash
docker-compose logs airflow | findstr "admin"
```


### 5. Запустить DAG

1. Открыть Airflow UI → `http://localhost:8080`
2. Найти DAG `sales_mart_etl`
3. Включить DAG (переключатель ON)
4. Нажать кнопку "Trigger DAG"
---

## Настройка

### Docker настройки

#### Dockerfile:
```dockerfile
FROM apache/airflow:3.3.1

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt
```

#### docker-compose.yml:
```yaml
x-airflow-common: &airflow-common
  image: apache/airflow:3.3.1
  environment:
    AIRFLOW__CORE__EXECUTOR: SequentialExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${POSTGRES_USER:-airflow}:${POSTGRES_PASSWORD:-airflow}@postgres/${POSTGRES_DB:-airflow}
    AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "false"
    AIRFLOW__API__AUTH_BACKENDS: airflow.api.auth.backend.basic_auth
    AIRFLOW__CORE__DAG_BUNDLE_NAME: dags-folder
    AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL: 30
    AIRFLOW_CONN_POSTGRES_DEFAULT: postgresql://airflow:airflow@postgres:5432/airflow
  volumes:
    - ./dags:/opt/airflow/dags
    - ./include:/opt/airflow/include
    - ./logs:/opt/airflow/logs
    - ./plugins:/opt/airflow/plugins
    - ./data:/opt/airflow/data
  depends_on:
    postgres:
      condition: service_healthy
  env_file:
    - .env

services:
  postgres:
    image: postgres:15-alpine
    environment:
      POSTGRES_USER: ${POSTGRES_USER:-airflow}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-airflow}
      POSTGRES_DB: ${POSTGRES_DB:-airflow}
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "${POSTGRES_USER:-airflow}"]
      interval: 10s
      timeout: 5s
      retries: 5
    restart: always

  db-init:
    <<: *airflow-common
    command: airflow db migrate
    restart: "no"
    depends_on:
      postgres:
        condition: service_healthy

  airflow:
    <<: *airflow-common
    command: standalone
    ports:
      - "8080:8080"
    healthcheck:
      test: ["CMD-SHELL", "curl --fail http://localhost:8080/ || exit 1"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    restart: always
    depends_on:
      postgres:
        condition: service_healthy
      db-init:
        condition: service_completed_successfully

volumes:
  postgres_data:
```

### Данные

Файлы должны находиться в `data/`:

```
data/
├── customers.csv                  # Файл с пользователями
└── orders_2026-08-27.csv          # Ежедневный файл (формат: orders_YYYY-MM-DD.csv)
```

**Пример `customers.csv`:**
```csv
customer_id,customer_name,segment,city
1,Иван Петров,VIP,Москва
2,Мария Сидорова,Regular,СПб
3,Алексей Иванов,New,Казань
4,Ольга Смирнова,VIP,Екатеринбург
```

**Пример `orders_2026-08-27.csv`:**
```csv
order_id,order_date,customer_id,product_name,quantity,unit_price,currency,status
1005,2026-08-27,1,Смартфон,3,75000,RUB,completed
1006,2026-08-27,2,Наушники,10,5000,RUB,completed
1007,2026-08-27,3,Монитор,2,45000,RUB,completed
1008,2026-08-27,4,Клавиатура,1,2400,RUB,completed
1009,2026-08-27,1,Ноутбук,1,120000,RUB,completed
1010,2026-08-27,2,Мышь,15,2000,RUB,completed
```

---

## Запуск DAG

### Расписание

DAG запускается ежедневно в **8:00**:

```python
schedule='0 8 * * *'
```
> 🕒 **Важно:** Время указано в UTC. Для Москвы (UTC+3) это **11:00**.

### Ручной запуск

1. Открой `http://localhost:8080`
2. Найди DAG `sales_mart_etl`
3. Нажми кнопку "Trigger DAG"
**Через CLI:**

```bash
docker exec -it airflow_project-airflow-1 airflow dags trigger sales_mart_etl
```

### Backfill (запуск за прошлые даты)

```bash
docker exec -it airflow_project-airflow-1 airflow dags backfill sales_mart_etl \
    --start-date 2026-08-20 \
    --end-date 2026-08-25
```

---

## Мониторинг и логи

### Логи контейнеров

```bash
# Логи Airflow
docker-compose logs airflow --tail=50

# Логи PostgreSQL
docker-compose logs postgres --tail=50
```

### Логи DAG в UI

1. Открыть DAG в Airflow UI
2. Нажать на конкретный Task
3. Выбрать **"Log"**

### Проверка данных в PostgreSQL

```bash
# Зайти в PostgreSQL
docker exec -it airflow_project-postgres-1 psql -U airflow -d airflow

# Просмотр данных
\dt staging.*;
SELECT * FROM staging.orders_raw LIMIT 10;
SELECT * FROM mart.daily_sales_mart LIMIT 10;
\q
```

### Проверка состояния

```bash
# Статус контейнеров
docker-compose ps

# Использование ресурсов
docker stats

# Проверить, что DAG загружен
docker exec -it airflow_project-airflow-1 airflow dags list
```

---

## CI/CD

Проект использует **GitHub Actions** для автоматической проверки:

1. **Pull Request** → Автоматическая проверка DAG
2. **Push в main** → Проверка синтаксиса и импорта

### Файл: `.github/workflows/ci-cd.yaml`

```yaml
name: Check Airflow DAG

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  check-dag:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v4
      
      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.10'
      
      - name: Install dependencies
        run: |
          pip install --upgrade pip
          pip install -r requirements.txt
      
      - name: Check Python syntax
        run: |
          python -m py_compile dags/sales_mart_etl.py
      
      - name: Check DAG can be imported
        run: |
          python -c "
          import sys
          import os
          sys.path.append(os.getcwd())
          
          from airflow import DAG
          import dags.sales_mart_etl
          
          print('✅ DAG imported successfully!')
          "
```

---

## Устранение неполадок

### ❌ Ошибка: `FileNotFoundError: /opt/airflow/data/orders_*.csv`

**Решение:** Проверьте, что файл существует:
```bash
docker exec -it airflow_project-airflow-1 ls -la /opt/airflow/data/
```

### ❌ Порт 8080 уже занят

**Решение:** Освободите или измените порт в `docker-compose.yml`:
```yaml
ports:
  - "8081:8080"  # ← Новый порт
```

### ❌ Ошибка: `DAG seems to be missing`

**Решение:** 
1. Проверьте, что файл находится в папке `dags/`
2. Проверьте ошибки импорта:
```bash
docker exec -it airflow_project-airflow-1 airflow dags list-import-errors
```
3. Перезапустите Airflow:
```bash
docker-compose restart airflow
```
---

## 📝 Полезные команды

### Управление контейнерами

```bash
# Запустить
docker-compose up -d

# Остановить
docker-compose down

# Перезапустить
docker-compose restart

# Пересобрать и запустить
docker-compose up -d --build

# Остановить и удалить все (включая данные)
docker-compose down -v
```

### Доступ к контейнеру Airflow

```bash
# Зайти в контейнер Airflow
docker exec -it airflow_project-airflow-1 bash

# Проверить установленные пакеты
docker exec -it airflow_project-airflow-1 pip list

# Проверить переменные окружения
docker exec airflow_project-airflow-1 env
```

### Работа с Airflow CLI

```bash
# Список всех DAG
docker exec -it airflow_project-airflow-1 airflow dags list
  
# Ошибки импорта DAG
docker exec -it airflow_project-airflow-1 airflow dags list-import-errors

# Статус запусков DAG
docker exec -it airflow_project-airflow-1 airflow dags list-runs sales_mart_etl

# Запустить DAG
docker exec -it airflow_project-airflow-1 airflow dags trigger sales_mart_etl

# Приостановить / возобновить DAG
docker exec -it airflow_project-airflow-1 airflow dags pause sales_mart_etl
docker exec -it airflow_project-airflow-1 airflow dags unpause sales_mart_etl

# Backfill (запуск за прошлые даты)
docker exec -it airflow_project-airflow-1 airflow dags backfill sales_mart_etl \
    --start-date 2026-08-20 \
    --end-date 2026-08-25
```

### Проверка данных в PostgreSQL

```bash
# Подключиться к PostgreSQL
docker exec -it airflow_project-postgres-1 psql -U airflow -d airflow
```

**Внутри `psql`:**

```sql
-- Посмотреть все таблицы в схемах
\dt staging.*;
\dt mart.*;

-- Проверить данные в staging
SELECT * FROM staging.orders_raw LIMIT 10;
SELECT * FROM staging.customers_raw LIMIT 10;

-- Проверить витрину
SELECT * FROM mart.daily_sales_mart LIMIT 10;

-- Выйти
\q
```

**Выполнить SQL одной командой (без входа в psql):**

```bash
# Проверить витрину
docker exec -it airflow_project-postgres-1 psql -U airflow -d airflow -c "SELECT * FROM mart.daily_sales_mart LIMIT 10;"

# Проверить количество записей
docker exec -it airflow_project-postgres-1 psql -U airflow -d airflow -c "SELECT COUNT(*) FROM staging.orders_raw;"
```

---

## 🔍 Диагностика проблем

```bash
# Посмотреть логи Airflow
docker-compose logs airflow --tail=50

# Посмотреть логи PostgreSQL
docker-compose logs postgres --tail=50
```

---

## 📦 Зависимости

### requirements.txt

```txt
apache-airflow==3.3.1
pandas==3.0.5
apache-airflow-providers-postgres==7.0.2
```

---

## 📞 Контакты

- **Автор:** [Александр]
- **Email:** [navselv3@yandex.ru]
- **GitHub:** [github.com/ViktorPetrovic](https://github.com/ViktorPetrovic)

---

## 🙏 Благодарности

- [Apache Airflow](https://airflow.apache.org/)
- [PostgreSQL](https://www.postgresql.org/)
- [Docker](https://www.docker.com/)


