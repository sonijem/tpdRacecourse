from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import json
import io
import logging
# Set up logging 
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# Define constants 
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
    """
    Get a Postgres connection using the PostgresHook.
    Returns:
        psycopg2 connection object
    """
    hook = PostgresHook(postgres_conn_id='tpd_postgres')  
    return hook.get_conn()

def upsert_names(cur, names, table):
    """
    Upsert names into the specified table.
    Args:
        cur: Postgres cursor
        names: Set of names to upsert
        table: Name of the table to upsert into (e.g., 'courses', 'horses')
    """
    schema_table = f"tpd_hourse_race.{table}"
    inserted_count = 0
    for name in names:
        cur.execute(f"""
            INSERT INTO {schema_table} ({table[:-1]}_name)
            VALUES (%s)
            ON CONFLICT ({table[:-1]}_name) DO NOTHING
        """, (name,))
        if cur.rowcount == 1:
            inserted_count += 1
    logger.info(f"Inserted {inserted_count} new records into {schema_table}.")

def process_chunk(df, is_runner):
    """
    Process a chunk of data and upsert into the database.
    Args:
        df: DataFrame containing the chunk of data
        is_runner: Boolean indicating if the chunk is for runners or racecards
    """
    with get_postgres_conn() as conn:
        cur = conn.cursor()
        inserted_count = 0
        if is_runner:
            for _, row in df.iterrows():
                logger.info(f"Processing runner: race_id={row['race_id']}, horse_name={row['horse_name']}, cloth_number={row['cloth_number']}")
                cur.execute("SELECT horse_id FROM tpd_hourse_race.horses WHERE horse_name = %s", (row['horse_name'],))
                horse = cur.fetchone()
                if horse is None:
                    logger.error(f"Horse '{row['horse_name']}' not found in horses table.")
                    raise ValueError(f"Horse '{row['horse_name']}' not found in horses table.")
                horse_id = horse[0]
                logger.info(f"Inserting runner: race_id={row['race_id']}, horse_id={horse_id}, cloth_number={row['cloth_number']}, starting_price={row['starting_price']}")
                cur.execute("""
                    INSERT INTO tpd_hourse_race.runners (race_id, horse_id, cloth_number, starting_price)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (race_id, cloth_number) DO NOTHING
                """, (row['race_id'], horse_id, int(row['cloth_number']), str(row['starting_price'])))
                if cur.rowcount == 1:
                    inserted_count += 1
        else:
            for _, row in df.iterrows():
                logger.info(f"Processing race: race_id={row['race_id']}, course_name={row['course_name']}, post_time={row['post_time']}")
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
                    cur.execute("SELECT course_id FROM tpd_hourse_race.courses WHERE course_name = %s", (row['course_name'],))
                    course = cur.fetchone()
                    if course is None:
                        logger.error(f"Course '{row['course_name']}' not found in courses table after upsert.")
                        raise ValueError(f"Course '{row['course_name']}' not found in courses table after upsert.")
                    course_id = course[0]
                logger.info(f"Inserting race: race_id={row['race_id']}, post_time={row['post_time']}, course_id={course_id}")
                cur.execute("""
                    INSERT INTO tpd_hourse_race.races (race_id, post_time, course_id)
                    VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (row['race_id'], row['post_time'], course_id))
                if cur.rowcount == 1:
                    inserted_count += 1
        logger.info(f"Inserted {inserted_count} new records into {'runners' if is_runner else 'races'} table for this chunk.")
        conn.commit()

def load_source1():
    """
    Load data from source1 CSV files and upsert into Postgres.
    """
    hook = GCSHook()
    logger.info("Downloading source1 racecards and runners data from GCS")
    racecards_data = hook.download(bucket_name, 'source1/source1_racecards.csv').decode('utf-8')
    runners_data = hook.download(bucket_name, 'source1/source1_runners.csv').decode('utf-8')

    logger.info("Upserting course names from racecards data from source1")
    racecards_df = pd.read_csv(io.StringIO(racecards_data))
    with get_postgres_conn() as conn:
        cur = conn.cursor()
        course_names = set(racecards_df['course_name'].unique())
        logger.info(f"Found {len(course_names)} unique course names to upsert")
        upsert_names(cur, course_names, 'courses')
        conn.commit()
        logger.info("Completed upserting course names")

    logger.info("Upserting horse names from runners data")
    runners_df = pd.read_csv(io.StringIO(runners_data))
    
    with get_postgres_conn() as conn:
        cur = conn.cursor()
        horse_names = set(runners_df['horse_name'].unique())
        logger.info(f"Found {len(horse_names)} unique horse names to upsert ")
        upsert_names(cur, horse_names, 'horses')
        conn.commit()
        logger.info("Completed upserting horse names")

    logger.info("Processing racecards and runners data in chunks")
    for data, is_runner in [(racecards_data, False), (runners_data, True)]:
        df = pd.read_csv(io.StringIO(data))
        
        for chunk in range(0, len(df), CHUNK_SIZE):
            process_chunk(df.iloc[chunk:chunk+CHUNK_SIZE], is_runner)

def load_source2():
    """
    Load data from source2 JSON files and upsert into Postgres.
    """
    hook = GCSHook()
    logger.info("Downloading source2 racecards and runners data from GCS")
    racecards_lines = hook.download(bucket_name, 'source2/source2_racecards.json').decode('utf-8').splitlines()
    runners_lines = hook.download(bucket_name, 'source2/source2_runners.json').decode('utf-8').splitlines()

    # Upsert all course_names from the full racecards file before chunking
    logger.info("Upserting course names from racecards data")
    racecards_objs = [json.loads(line) for line in racecards_lines]
    racecards_df = pd.DataFrame(racecards_objs)
    with get_postgres_conn() as conn:
        cur = conn.cursor()
        course_names = set(racecards_df['course_name'].unique())
        logger.info(f"Found {len(course_names)} unique course names to upsert")
        upsert_names(cur, course_names, 'courses')
        conn.commit()
        logger.info("Completed upserting course names")

    logger.info("Upserting horse names from runners data")
    # Upsert all horse_names from the full runners file before chunking
    runners_objs = [json.loads(line) for line in runners_lines]
    runners_df = pd.DataFrame(runners_objs)
    with get_postgres_conn() as conn:
        cur = conn.cursor()
        horse_names = set(runners_df['horse_name'].unique())
        logger.info(f"Found {len(horse_names)} unique horse names to upsert")
        upsert_names(cur, horse_names, 'horses')
        conn.commit()
        logger.info("Completed upserting horse names")

    # Now process in chunks as before
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
