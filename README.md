## Total Peroformance Data

### Orchestation Framework

I would use GCP to host and implement TPD's data pipelines.

#### Composer
- Created Composer 2 env: **tpd-uk** with image composer-2.13.1-airflow-2.10.5
- Airflow env user updated to Admin using below command

    gcloud composer environments run tpd-uk \
    --location europe-west2 \
    users add-role -- -e sonidharti12345@gmail.com -r Admin
- Composer created europe-west2-tpd-uk-1250b41b-bucket bucket and stored all the dags in /dags folder.
- Connection **tpd_postgres** created for postgres in Airflow ui

#### GCS
- Created bucket **data-engineering-ingestion** that will be used to store data retrieved from different sources.
- Data gets stored in format : source_name/source_files


#### Postgresql on Cloud SQL
- Created postgresql database instance : **tpd-postgres-uk**.
- Created Schema, tables and indexes using file tpd_racecources.sql.

### DAG explanation

- For each source download the files
- Upsert all course_name into course table
- Upsert all hourse_name into horse table
- Insert data in chunks for races and runner table by getting relavant fkeys.
- Added logs for better understanding of the code implementation!

### Analytics queries

- Analytics queries added into file **tpd_racecourses_analytics.sql**