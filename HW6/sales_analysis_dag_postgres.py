"""
DAG для анализа продаж и клиентов

- Загружает данные из CSV в PostgreSQL
- Выполняет аналитические запросы
- Проверяет результаты и отправляет уведомления
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os
import logging


# Пути к файлам
DATA_DIR = '/opt/airflow/data'
OUTPUT_DIR = '/opt/airflow/output'


POSTGRES_CONN_ID = 'postgres_default'

ALERT_EMAIL = 'yukata66655@gmail.com'


def get_postgres_engine():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    return hook.get_sqlalchemy_engine()



def load_customer_data():
    """Загрузка данных о клиентах"""
    logging.info("Загрузка customer.csv в PostgreSQL...")
    
    # читаем CSV 
    df = pd.read_csv(
        f'{DATA_DIR}/customer.csv',
        sep=';',
        encoding='utf-8'
    )
    
    # получаем engine для постгрес
    engine = get_postgres_engine()
    
    # загружаем в таблицу (если таблица существует,то заменяем)
    df.to_sql(
        'customer',
        engine,
        if_exists='replace',
        index=False,
        method='multi',  
        chunksize=1000
    )
    
    logging.info(f"Загружено {len(df)} записей в таблицу customer")


def load_product_data():
    """Загрузка данных о продуктах"""
    logging.info("Загрузка product.csv в PostgreSQL...")
    
    df = pd.read_csv(
        f'{DATA_DIR}/product.csv',
        encoding='utf-8'
    )
    
    engine = get_postgres_engine()
    
    df.to_sql(
        'product',
        engine,
        if_exists='replace',
        index=False,
        method='multi',
        chunksize=1000
    )
    
    logging.info(f"Загружено {len(df)} записей в таблицу product")


def load_orders_data():
    """Загрузка данных о заказах"""
    logging.info("Загрузка orders.csv в PostgreSQL...")
    
    df = pd.read_csv(
        f'{DATA_DIR}/orders.csv',
        encoding='utf-8'
    )
    
    engine = get_postgres_engine()
    
    df.to_sql(
        'orders',
        engine,
        if_exists='replace',
        index=False,
        method='multi',
        chunksize=1000
    )
    
    logging.info(f"Загружено {len(df)} записей в таблицу orders")


def load_order_items_data():
    """Загрузка данных о позициях заказов"""
    logging.info("Загрузка order_items.csv в PostgreSQL...")
    
    df = pd.read_csv(
        f'{DATA_DIR}/order_items.csv',
        encoding='utf-8'
    )
    
    engine = get_postgres_engine()
    
    df.to_sql(
        'order_items',
        engine,
        if_exists='replace',
        index=False,
        method='multi',
        chunksize=1000
    )
    
    logging.info(f"Загружено {len(df)} записей в таблицу order_items")


def query_top_customers():
    """
    ТОП-3 клиентов с минимальной и максимальной суммой транзакций
    """
    logging.info("Выполнение запроса: ТОП клиентов по сумме транзакций...")
    
    query = """
    WITH customer_totals AS (
        SELECT 
            c.customer_id,
            c.first_name,
            c.last_name,
            COALESCE(SUM(oi.quantity * oi.item_list_price_at_sale), 0) as total_amount
        FROM customer c
        LEFT JOIN orders o ON c.customer_id = o.customer_id
        LEFT JOIN order_items oi ON o.order_id = oi.order_id
        GROUP BY c.customer_id, c.first_name, c.last_name
    ),
    ranked AS (
        SELECT 
            customer_id,
            first_name,
            last_name,
            total_amount,
            ROW_NUMBER() OVER (ORDER BY total_amount ASC) as rank_asc,
            ROW_NUMBER() OVER (ORDER BY total_amount DESC) as rank_desc
        FROM customer_totals
    )
    SELECT 
        first_name,
        last_name,
        total_amount,
        CASE 
            WHEN rank_asc <= 3 THEN 'TOP-3 MIN'
            WHEN rank_desc <= 3 THEN 'TOP-3 MAX'
        END as category
    FROM ranked
    WHERE rank_asc <= 3 OR rank_desc <= 3
    ORDER BY total_amount DESC
    """
    
    engine = get_postgres_engine()
    df = pd.read_sql_query(query, engine)
    
    # Создаём папку output если не существует
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Сохраняем результат
    output_file = f'{OUTPUT_DIR}/top_customers.csv'
    df.to_csv(output_file, index=False)
    
    logging.info(f"Результат сохранён в {output_file}, строк: {len(df)}")
    return len(df)


def query_wealth_segments():
    """
    Запрос 2: ТОП-5 клиентов (по общему доходу) в каждом сегменте благосостояния
    """
    logging.info("Выполнение запроса: ТОП-5 по сегментам благосостояния...")
    
    query = """
    WITH customer_revenue AS (
        SELECT 
            c.customer_id,
            c.first_name,
            c.last_name,
            c.wealth_segment,
            COALESCE(SUM(oi.quantity * oi.item_list_price_at_sale), 0) as total_revenue
        FROM customer c
        LEFT JOIN orders o ON c.customer_id = o.customer_id
        LEFT JOIN order_items oi ON o.order_id = oi.order_id
        GROUP BY c.customer_id, c.first_name, c.last_name, c.wealth_segment
    ),
    ranked AS (
        SELECT 
            first_name,
            last_name,
            wealth_segment,
            total_revenue,
            ROW_NUMBER() OVER (PARTITION BY wealth_segment ORDER BY total_revenue DESC) as rank
        FROM customer_revenue
    )
    SELECT 
        first_name,
        last_name,
        wealth_segment,
        total_revenue
    FROM ranked
    WHERE rank <= 5
    ORDER BY wealth_segment, total_revenue DESC
    """
    
    engine = get_postgres_engine()
    df = pd.read_sql_query(query, engine)
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    output_file = f'{OUTPUT_DIR}/wealth_segments_top5.csv'
    df.to_csv(output_file, index=False)
    
    logging.info(f"Результат сохранён в {output_file}, строк: {len(df)}")
    return len(df)


# проверяем строки

def check_query_results(**context):
    """
    Проверяем. что запросы вернули данные
    Если хотя бы один запрос вернул 0 строк, то генерируем ошибку
    """
    logging.info("Проверка результатов запросов...")
    
    # проверяем файлы
    file1 = f'{OUTPUT_DIR}/top_customers.csv'
    file2 = f'{OUTPUT_DIR}/wealth_segments_top5.csv'
    
    results = []
    
    if os.path.exists(file1):
        df1 = pd.read_csv(file1)
        count1 = len(df1)
        results.append(('top_customers', count1))
        logging.info(f"top_customers.csv: {count1} строк")
    else:
        results.append(('top_customers', 0))
        logging.warning("Файл top_customers.csv не найден!")
    
    if os.path.exists(file2):
        df2 = pd.read_csv(file2)
        count2 = len(df2)
        results.append(('wealth_segments_top5', count2))
        logging.info(f"wealth_segments_top5.csv: {count2} строк")
    else:
        results.append(('wealth_segments_top5', 0))
        logging.warning("Файл wealth_segments_top5.csv не найден!")
    
    # проверяем результаты
    failed = [name for name, count in results if count == 0]
    
    if failed:
        error_msg = f"Запросы вернули 0 строк: {', '.join(failed)}"
        logging.error(error_msg)
        # сохраняем ошибку для отправки птсьма на почту
        context['ti'].xcom_push(key='error_message', value=error_msg)
        raise ValueError(error_msg)
    else:
        logging.info("Все запросы выполнены успешно!")
        return True


def print_success():
    """Вывод сообщения об успешном выполнении"""
    logging.info("=" * 30)
    logging.info("DAG ВЫПОЛНЕН УСПЕШНО!")
    logging.info("Все данные загружены в PostgreSQL и обработаны")
    logging.info("Результаты сохранены в /opt/airflow/output/")
    logging.info("=" * 30)


def print_failure(**context):
    """Вывод сообщения о неудачном выполнении"""
    error_msg = context['ti'].xcom_pull(key='error_message')
    
    logging.error("=" * 30)
    logging.error("DAG ЗАВЕРШЁН С ОШИБКОЙ!")
    logging.error(f"Причина: {error_msg}")
    logging.error("=" * 30)



# НАСТРОЙКА DAG


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 21),
    'email': [ALERT_EMAIL],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# создаём DAG
dag = DAG(
    'sales_analysis_postgres',
    default_args=default_args,
    description='Ежедневный анализ продаж и клиентов (PostgreSQL)',
    schedule_interval='@daily',  # запуск каждый день
    catchup=False,
    tags=['sales', 'analytics', 'etl', 'postgres'],
)


# ОПРЕДЕЛЕНИЕ ЗАДАЧ


# стартовая задача
start = DummyOperator(
    task_id='start',
    dag=dag,
)

# з=адачи загрузки данных (выполняются параллельно)
load_customer = PythonOperator(
    task_id='load_customer',
    python_callable=load_customer_data,
    dag=dag,
)

load_product = PythonOperator(
    task_id='load_product',
    python_callable=load_product_data,
    dag=dag,
)

load_orders = PythonOperator(
    task_id='load_orders',
    python_callable=load_orders_data,
    dag=dag,
)

load_order_items = PythonOperator(
    task_id='load_order_items',
    python_callable=load_order_items_data,
    dag=dag,
)

# синхронизацич после загрузки
join_loads = DummyOperator(
    task_id='join_loads',
    dag=dag,
)

# задачи выполнения запросов (параллельно)
query_top = PythonOperator(
    task_id='query_top_customers',
    python_callable=query_top_customers,
    dag=dag,
)

query_wealth = PythonOperator(
    task_id='query_wealth_segments',
    python_callable=query_wealth_segments,
    dag=dag,
)

# синхронизация после запросов
join_queries = DummyOperator(
    task_id='join_queries',
    dag=dag,
)

# проверка результатов
check = PythonOperator(
    task_id='check_results',
    python_callable=check_query_results,
    provide_context=True,
    dag=dag,
)

# письмо при ошибке (запускается если check упал)
send_alert = EmailOperator(
    task_id='send_failure_email',
    to=ALERT_EMAIL,
    subject='[AIRFLOW] Sales Analysis DAG Failed',
    html_content="""
    <h3>DAG sales_analysis_postgres завершился с ошибкой</h3>
    <p>Один или несколько запросов вернули 0 строк.</p>
    <p>Проверьте логи Airflow для деталей.</p>
    <p>Время: {{ ds }}</p>
    """,
    dag=dag,
    trigger_rule='one_failed',
)

# сообщение об успехе
success = PythonOperator(
    task_id='print_success',
    python_callable=print_success,
    dag=dag,
    trigger_rule='all_success',  # запускается только если всё норм
)

# сообщение об ошибке
failure = PythonOperator(
    task_id='print_failure',
    python_callable=print_failure,
    provide_context=True,
    dag=dag,
    trigger_rule='one_failed',  # запускается если хлтя бы что-то упало
)


# ГРАФ ЗАВИСИМОСТЕЙ


# Загрузка данных
start >> [load_customer, load_product, load_orders, load_order_items]

# после загрузки  синхронизация
[load_customer, load_product, load_orders, load_order_items] >> join_loads

# выполнение запросов
join_loads >> [query_top, query_wealth]

# после запросов синхронизация
[query_top, query_wealth] >> join_queries

# проверка результатов
join_queries >> check

# после проверки разные результаты
check >> [success, send_alert, failure]
