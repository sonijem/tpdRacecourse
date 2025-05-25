from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import json
import io

# Config (should ideally be moved to Airflow Connections or Secret Manager)
PG_CONFIG = {
    'host': '34.89.77.153',
    'port': 5432,
    'dbname': 'tpd-postgres-uk',  
    'user': 'postgres', 
    'password': 'Godstay1!'  
}
bucket_name = 'data-engineering-ingestion'
CHUNK_SIZE = 1000

default_args = {'start_date': datetime(2025, 2, 1), 'retries': 1}
dag = DAG(
    dag_id='gcs_to_postgres_chunked_ingest',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False
)

def get_postgres_conn():
    # Use Airflow's PostgresHook for connection management
    hook = PostgresHook(postgres_conn_id='tpd_postgres')  # Set this Airflow connection in UI
    return hook.get_conn()

def upsert_names(cur, names, table):
    for name in names:
        cur.execute(f"""
            INSERT INTO {table} ({table[:-1]}_name)
            VALUES (%s)
            ON CONFLICT ({table[:-1]}_name) DO NOTHING
        """, (name,))

def process_chunk(df, is_runner):
    with get_postgres_conn() as conn:
        cur = conn.cursor()

        if is_runner:
            horse_names = set(df['horse_name'].unique())
            upsert_names(cur, horse_names, 'horses')

            for _, row in df.iterrows():
                cur.execute("SELECT horse_id FROM tpd_hourse_race.horses WHERE horse_name = %s", (row['horse_name'],))
                horse_id = cur.fetchone()[0]
                cur.execute("""
                    INSERT INTO tpd_hourse_race.runners (race_id, horse_id, cloth_number, starting_price)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (race_id, cloth_number) DO NOTHING
                """, (row['race_id'], horse_id, int(row['cloth_number']), str(row['starting_price'])))
        else:
            course_names = set(df['course_name'].unique())
            upsert_names(cur, course_names, 'courses')

            for _, row in df.iterrows():
                cur.execute("SELECT course_id FROM tpd_hourse_race.courses WHERE course_name = %s", (row['course_name'],))
                course_id = cur.fetchone()[0]
                cur.execute("""
                    INSERT INTO races (race_id, post_time, course_id)
                    VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (row['race_id'], row['post_time'], course_id))

        conn.commit()

def load_source1():
    hook = GCSHook()
    racecards_data = hook.download(bucket_name, 'source1/source1_racecards.csv').decode('utf-8')
    runners_data = hook.download(bucket_name, 'source1/source1_runners.csv').decode('utf-8')

    for data, is_runner in [(racecards_data, False), (runners_data, True)]:
        df = pd.read_csv(io.StringIO(data))
        for chunk in range(0, len(df), CHUNK_SIZE):
            process_chunk(df.iloc[chunk:chunk+CHUNK_SIZE], is_runner)

def load_source2():
    hook = GCSHook()
    racecards_lines = hook.download(bucket_name, 'source2/source2_racecards.json').decode('utf-8').splitlines()
    runners_lines = hook.download(bucket_name, 'source2/source2_runners.json').decode('utf-8').splitlines()

    for lines, is_runner in [(racecards_lines, False), (runners_lines, True)]:
        for chunk in range(0, len(lines), CHUNK_SIZE):
            json_objects = [json.loads(line) for line in lines[chunk:chunk+CHUNK_SIZE]]
            df = pd.DataFrame(json_objects)
            process_chunk(df, is_runner)

PythonOperator(
    task_id='load_source1_csv',
    python_callable=load_source1,
    dag=dag
)

PythonOperator(
    task_id='load_source2_json',
    python_callable=load_source2,
    dag=dag
)
