#####################################
# DAG for TCRM auditing purpose
# which maintains the records
# ingested on daily basis
#####################################

from airflow import DAG
import boto3
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import logging
import psycopg2
import snowflake.connector
from custom.common.email_alert import email_alert
import pandas as pd
import io

default_args = {
    'owner': 'data-and-analytics',
    'start_date': datetime(2022, 5, 24),
    'provide_context': True,
    'retries': 0,
    'max_active_runs': 1,
}

dag = DAG(
    dag_id='audit_tcrm',
    default_args=default_args,
    catchup=False,
    schedule_interval='0 9 * * *',
    on_failure_callback=email_alert
)

ssm_client = boto3.client('ssm', region_name='us-east-1')
env_param = ssm_client.get_parameter(Name='env', WithDecryption=True)
env = env_param.get('Parameter').get('Value')


def ssm_get_audit_data():
    session = boto3.session.Session()
    ssm_client = session.client('ssm', region_name='us-east-1')

    rds_host_param = ssm_client.get_parameter(
        Name='/users/governance_rds/endpoint', WithDecryption=True)
    rds_host_value = rds_host_param.get('Parameter').get('Value')

    rds_database_value = 'warehouse'

    rds_user_param = ssm_client.get_parameter(
        Name='/users/governance_rds/user', WithDecryption=True)
    rds_user_value = rds_user_param.get('Parameter').get('Value')

    rds_pw_param = ssm_client.get_parameter(
        Name='/users/governance_rds/password', WithDecryption=True)
    rds_pw_value = rds_pw_param.get('Parameter').get('Value')

    conn_params = {'host': rds_host_value, 'database': rds_database_value,
                   'user': rds_user_value, 'password': rds_pw_value}
    logging.info("Passing RDS Credentials.")

    return conn_params

# getting snowflake information for passing values from parameter store


def get_snowflake_info():

    ssm_client = boto3.client('ssm', region_name='us-east-1')
    logging.info('Getting env info')
    env_param = ssm_client.get_parameter(Name='env', WithDecryption=True)
    env_value = env_param.get('Parameter').get('Value')
    logging.info('Getting snowflake variables')

    snf_acc_param = ssm_client.get_parameter(
        Name='/users/snowflake/account', WithDecryption=True)
    snf_acc_value = snf_acc_param.get('Parameter').get('Value')

    snf_user_param = ssm_client.get_parameter(
        Name='/users/snowflake/account/user', WithDecryption=True)
    snf_user_value = snf_user_param.get('Parameter').get('Value')

    snf_key_param = ssm_client.get_parameter(
        Name='/users/snowflake/account/password', WithDecryption=True)
    snf_key_value = snf_key_param.get('Parameter').get('Value')
    env_var = {
        'env': env_value,
        'snf_account': snf_acc_value,
        'snf_user': snf_user_value,
        'snf_key': snf_key_value
    }
    env_var['warehouse'] = 'AWS_WH'
    env_var['database'] = f'{env}_RZ'
    env_var['schema'] = 'INVH_SALESFORCE'

    logging.info("Passing Snowflake Credentials.")

    return env_var


def audit_capture(ob_name, date_col, *args, **kwargs):
    try:
        # task_id='daily_audit_email'
        audit_conn_params = ssm_get_audit_data()

        # Postgres Connection
        conn_args = dict(
            host=audit_conn_params['host'],
            database=audit_conn_params['database'],
            user=audit_conn_params['user'],
            password=audit_conn_params['password'],
            port='5432')
        conn = psycopg2.connect(**conn_args)
        audit_cur = conn.cursor()

        snowflake_credentials = get_snowflake_info()

        # Snowflake Connection
        connection = snowflake.connector.connect(
            user=snowflake_credentials['snf_user'],
            password=snowflake_credentials['snf_key'],
            account=snowflake_credentials['snf_account'],
            warehouse=snowflake_credentials['warehouse'],
            database=snowflake_credentials['database'],
            schema=snowflake_credentials['schema'],
            role='sysadmin',
        )
        snowflake_cursor = connection.cursor()
        snowflake_query = ("WITH T AS " +
                           "(" +
                           "select " +
                           " '" + ob_name + "'" +
                           "as TABLE_NAME," +
                           "to_date(" +
                           " " + date_col + ") as DATE," +
                           " count(*) as INGESTED_ROWS " +
                           " from "+env+"_RZ.INVH_SALESFORCE." +
                           "" + ob_name +
                           " where DATE = current_date()-1"
                           " group by DATE " +
                           "    order by DATE desc, TABLE_NAME " +
                           " )" +
                           "SELECT current_timestamp AS job_start_ts," +
                           " current_timestamp AS job_end_ts," +
                           " 'INVH_SALESFORCE' as schema_name," +
                           " TABLE_NAME as table_name," +
                           " 'task_"+ob_name + "' as task_id," +
                           " 'TCRM' as job_type," +
                           " 'AUDIT_TCRM' as dag_id," +
                           "DATE as dag_run_exec_date," +
                           "'SUCCESS' AS job_status," +
                           " 0 AS recs_parsed," +
                           " 0 AS recs_staged," +
                           " INGESTED_ROWS AS recs_inserted," +
                           " 0 AS recs_updated," +
                           " 0 AS recs_deleted," +
                           " 'NULL' AS ERROR_MESSAGE" +
                           " FROM T"
                           )

        # print(snowflake_query)
        snowflake_cursor.execute(snowflake_query)
        audit_data = snowflake_cursor.fetchall()

        if (len(audit_data) != 0):
            for i in range(len(audit_data)):
                logging.info(type({audit_data[i][0]}))

                sql_query = f'''INSERT INTO etlaudit.ETL_JOB_STATUS(job_id,
                            job_start_ts,
                            job_end_ts,
                            schema_name,
                            table_name,
                            task_id,
                            job_type,
                            dag_id,
                            dag_run_exec_date,
                            job_status,
                            recs_parsed,
                            recs_staged,
                            recs_inserted,
                            recs_updated,
                            recs_deleted,
                            failure_reason) values
                            (nextval('etlaudit.etl_job_id_seq'),
                                '{audit_data[i][0]}',
                                '{audit_data[i][1]}',
                                '{audit_data[i][2]}',
                                '{audit_data[i][3]}',
                                '{audit_data[i][4]}',
                                '{audit_data[i][5]}',
                                '{audit_data[i][6]}',
                                '{audit_data[i][7]}',
                                '{audit_data[i][8]}',
                                '{audit_data[i][9]}',
                                {audit_data[i][10]},
                                {audit_data[i][11]},
                                {audit_data[i][12]},
                                {audit_data[i][13]},
                                '{audit_data[i][14]}') '''
                audit_cur.execute(sql_query)
                audit_cur.execute("COMMIT")

        audit_cur.close()
    except Exception as err:
        raise (err)


# audit_conn_params = TCRM_AUDIT.ssm_get_audit_data()
daily_audit_email = PythonOperator(
    task_id='daily_audit_email',
    python_callable=ssm_get_audit_data,
    provide_context=True,
    dag=dag
)

snowflake_info = PythonOperator(
    task_id='snowflake_info',
    python_callable=get_snowflake_info,
    provide_context=True,
    dag=dag
)

# Get S3 Bucket Info
s3_bucket = boto3.client('s3', region_name='us-east-1')
config_bucket_name = f'datalake-jobs-code-{env}-invh'
# Get Config File Information
config_file = 'dna-datalake-airflow/dag/tcrm_objects.txt'
config_obj = s3_bucket.get_object(
    Bucket=config_bucket_name,
    Key=config_file
)
config_df = pd.read_csv(io.BytesIO(config_obj['Body'].read()), sep="|")


# config_df = pd.DataFrame()
a = list()
for OBJECT_NAME, DATE_COLUMN in zip(config_df['OBJECT_NAME'],
                                    config_df['DATE_COLUMN']):
    test = PythonOperator(
        task_id='task_'+OBJECT_NAME,
        python_callable=audit_capture,
        op_args=[OBJECT_NAME,
                 DATE_COLUMN],
        provide_context=True,
        dag=dag
    )
    a.append(test)
    if len(a) > 0:
        daily_audit_email >> snowflake_info >> a
    else:
        daily_audit_email >> snowflake_info