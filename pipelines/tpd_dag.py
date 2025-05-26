from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import json
import io

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
    hook = PostgresHook(postgres_conn_id='tpd_postgres')  
    return hook.get_conn()

def upsert_names(cur, names, table):
    schema_table = f"tpd_hourse_race.{table}"
    for name in names:
        cur.execute(f"""
            INSERT INTO {schema_table} ({table[:-1]}_name)
            VALUES (%s)
            ON CONFLICT ({table[:-1]}_name) DO NOTHING
        """, (name,))

def process_chunk(df, is_runner):
    with get_postgres_conn() as conn:
        cur = conn.cursor()

        if is_runner:
            horse_names = set(df['horse_name'].unique())
            upsert_names(cur, horse_names, 'horses')
            conn.commit()

            for _, row in df.iterrows():
                # Always fetch horse_id after upsert
                cur.execute("SELECT horse_id FROM tpd_hourse_race.horses WHERE horse_name = %s", (row['horse_name'],))
                horse = cur.fetchone()
                if horse is None:
                    raise ValueError(f"Horse '{row['horse_name']}' not found in horses table.")
                horse_id = horse[0]
                cur.execute("""
                    INSERT INTO tpd_hourse_race.runners (race_id, horse_id, cloth_number, starting_price)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (race_id, cloth_number) DO NOTHING
                """, (row['race_id'], horse_id, int(row['cloth_number']), str(row['starting_price'])))
        else:
            course_names = set(df['course_name'].unique())
            upsert_names(cur, course_names, 'courses')
            conn.commit()

            for _, row in df.iterrows():
                # Use INSERT ... ON CONFLICT DO NOTHING RETURNING course_id
                cur.execute("""
                    INSERT INTO tpd_hourse_race.courses (course_name)
                    VALUES (%s)
                    ON CONFLICT (course_name) DO NOTHING
                    RETURNING course_id
                """, (row['course_name'],))
                result = cur.fetchone()
                if result:
                    course_id = result[0]
                else:
                    # If not inserted, fetch the existing course_id
                    cur.execute("SELECT course_id FROM tpd_hourse_race.courses WHERE course_name = %s", (row['course_name'],))
                    course = cur.fetchone()
                    if course is None:
                        raise ValueError(f"Course '{row['course_name']}' not found in courses table after upsert.")
                    course_id = course[0]
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
