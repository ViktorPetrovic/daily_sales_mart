import logging
import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

default_args = {
    "owner": "me",
    "depends_on_past": False,
    "start_date": datetime(2026, 8, 20),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}


def check_files_exist(target_date, **context):
    files_to_check = [
        f"/opt/airflow/data/orders_{target_date}.csv",
        "/opt/airflow/data/customers.csv",
    ]
    for file_path in files_to_check:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Файл не найден: {file_path}")
        logging.info(f"Файл найден: {file_path}")


def load_csv_to_postgres(file_path, table_name, target_date, **context):
    try:
        logging.info(f"Загрузка данных в таблицу {table_name} за {target_date}")
        hook = PostgresHook(postgres_conn_id="postgres_default")
        engine = hook.get_sqlalchemy_engine()

        df = pd.read_csv(file_path)
        if df.empty:
            logging.warning(f"Файл {file_path} пуст")
            return

        ds_date = pd.to_datetime(target_date).date()
        df["load_date"] = ds_date

        if "order_date" in df.columns:
            df["order_date"] = pd.to_datetime(df["order_date"]).dt.date

        delete_sql = f"TRUNCATE TABLE staging.{table_name}"
        hook.run(delete_sql)
        logging.info(f"Удалены данные из {table_name}")

        df.to_sql(
            name=table_name,
            con=engine,
            schema="staging",
            if_exists="append",
            index=False,
        )
        logging.info(f"Загружено {len(df)} строк")

    except Exception as e:
        logging.error(f"Ошибка: {e!s}")
        raise


def check_data_quality(target_date, **context):
    try:
        hook = PostgresHook(postgres_conn_id="postgres_default")

        sql_check_exists = (
            "SELECT COUNT(*) FROM staging.orders_raw WHERE order_date = %s::date"
        )
        count = hook.get_first(sql_check_exists, (target_date,))[0]

        if count == 0:
            logging.warning(f"Нет данных за {target_date} для проверки качества")
            return

        sql_null_check = """
            SELECT 
                COUNT(CASE WHEN customer_id IS NULL THEN 1 END),
                COUNT(CASE WHEN order_date IS NULL THEN 1 END),
                COUNT(CASE WHEN quantity IS NULL THEN 1 END),
                COUNT(CASE WHEN unit_price IS NULL THEN 1 END)
            FROM staging.orders_raw WHERE order_date = %s::date
        """
        result = hook.get_first(sql_null_check, (target_date,))
        if result and any(x > 0 for x in result):
            raise ValueError(f"Найдены NULL значения: {result}")

        sql_negative_check = """
            SELECT COUNT(*) FROM staging.orders_raw
            WHERE order_date = %s::date AND (quantity <= 0 OR unit_price <= 0)
        """
        negative_count = hook.get_first(sql_negative_check, (target_date,))[0]
        if negative_count > 0:
            raise ValueError(f"Найдены отрицательные значения: {negative_count}")

        logging.info("Проверка качества данных прошла успешно")
    except Exception as e:
        logging.error(f"Ошибка проверки: {e}")
        raise


def show_mart_data(target_date, **context):
    try:
        hook = PostgresHook(postgres_conn_id="postgres_default")
        sql = """
                SELECT sale_date, customer_name, segment, product_name, quantity, revenue_rub
                FROM mart.daily_sales_mart WHERE sale_date = %s::date
                ORDER BY revenue_rub DESC
        """
        result = hook.get_records(sql, parameters=(target_date,))

        if not result:
            logging.info(f"Нет данных в витрине за {target_date}")
            return

        logging.info("Витрина продаж:")
        logging.info(
            f"{'Дата':<10} | {'Клиент':<20} | {'Сегмент':<8} | {'Товар':<20} | {'Кол-во':<8} | {'Выручка':<12} ₽"
        )

        for row in result:
            sale_date = str(row[0])
            customer = row[1] if row[1] else "Неизвестно"
            segment = row[2] if row[2] else "Неизвестно"
            product = row[3] if row[3] else "Неизвестно"
            quantity = row[4] or 0
            total = row[5] or 0
            logging.info(
                f"{sale_date:<10} | {customer:<20} | {segment:<8} | {product:<20} | {quantity:<8} | {total:<12} ₽"
            )
    except Exception as e:
        logging.error(f"Ошибка вывода витрины {e}")
        raise


with DAG(
    dag_id="sales_mart_etl",
    default_args=default_args,
    description="Создание ETL-витрины продаж",
    max_active_runs=1,
    schedule="0 8 * * *",
    catchup=False,
    template_searchpath=["/opt/airflow/include/sql"],
    is_paused_upon_creation=True,
) as dag:
    yesterday_ds = (
        "{{ (logical_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"
    )

    check_files = PythonOperator(
        task_id="check_files",
        python_callable=check_files_exist,
        op_kwargs={"target_date": yesterday_ds},
    )

    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables",
        conn_id="postgres_default",
        sql="create_table.sql",
    )

    load_orders = PythonOperator(
        task_id="load_orders",
        python_callable=load_csv_to_postgres,
        op_kwargs={
            "file_path": f"/opt/airflow/data/orders_{yesterday_ds}.csv",
            "table_name": "orders_raw",
            "target_date": yesterday_ds,
        },
    )

    load_customers = PythonOperator(
        task_id="load_customers",
        python_callable=load_csv_to_postgres,
        op_kwargs={
            "file_path": "/opt/airflow/data/customers.csv",
            "table_name": "customers_raw",
            "target_date": yesterday_ds,
        },
    )

    quality_check = PythonOperator(
        task_id="quality_check",
        python_callable=check_data_quality,
        op_kwargs={"target_date": yesterday_ds},
    )

    fill_mart = SQLExecuteQueryOperator(
        task_id="fill_mart",
        conn_id="postgres_default",
        sql="transform_mart.sql",
    )

    show_mart = PythonOperator(
        task_id="show_mart_data",
        python_callable=show_mart_data,
        op_kwargs={"target_date": yesterday_ds},
    )

    check_files >> create_tables
    create_tables >> [load_customers, load_orders]
    [load_customers, load_orders] >> quality_check
    quality_check >> fill_mart >> show_mart
