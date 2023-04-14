from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.operators.s3_copy_object_operator import S3CopyObjectOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 4, 4),
    'retries': 0
}

dag = DAG('s3_to_snowflake', default_args=default_args, schedule_interval=None)

s3_to_snowflake = S3CopyObjectOperator(
    task_id='s3_to_snowflake',
    source_bucket_key='s3://bucket_name/file_name.csv',
    dest_bucket_key='snowflake://account_name/schema_name.table_name',
    aws_conn_id='aws_default',
    snowflake_conn_id='snowflake_default',
    dag=dag
)

snowflake_load = SnowflakeOperator(
    task_id='snowflake_load',
    sql='COPY INTO table_name FROM @stage_name/file_name.csv',
    snowflake_conn_id='snowflake_default',
    dag=dag
)

s3_to_snowflake >> snowflake_load


'''This code defines an Airflow DAG named "s3_to_snowflake" that has two tasks:
"s3_to_snowflake": A task that copies a file from an S3 bucket to a Snowflake table using the "S3CopyObjectOperator" 
and "SnowflakeOperator" operators. This task uses the AWS and Snowflake connections specified by the "aws_default" 
and "snowflake_default" connection IDs, respectively.
"snowflake_load": A task that loads data into a Snowflake table using the "SnowflakeOperator" operator. 
This task uses the Snowflake connection specified by the "snowflake_default" connection ID.
The two tasks are connected with a ">>" operator, which indicates that "s3_to_snowflake" should be executed before "snowflake_load".
The DAG has the following default arguments:
'owner': The owner of the DAG (set to 'airflow' in this case).
'depends_on_past': If the DAG should depend on past execution dates.
'start_date': The date from which the DAG should start running.
'retries': The number of times to retry the DAG in case of failure.'''