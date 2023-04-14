import logging
import smtplib
from datetime import date, datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import boto3
import pandas as pd
import pytz
from airflow import DAG
from airflow.configuration import conf
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from custom.common.email_alert import email_alert
from snowflake.sqlalchemy import URL
import snowflake.connector
from sqlalchemy import create_engine
import pyodbc
import pymssql
import numpy as np
from simple_salesforce import Salesforce

default_args = {
    'owner': 'data-and-analytics',
    'start_date': datetime(2022, 12, 18),
    'provide_context': True,
    'retries': 0,
    'max_active_runs': 1,
}

dag = DAG(
    'daily-validation-report-new',
    default_args=default_args,
    catchup=False,
    schedule_interval='0 15 * * *',
    on_failure_callback=email_alert,
    dagrun_timeout=timedelta(minutes=20)
)


# get boto3 client for required service
def _get_boto_client(service_name):
    client = boto3.client(service_name, region_name='us-east-1')
    return client


# get decrypt value of mentioned parameter store
def _get_ssm_value(name):
    client = _get_boto_client('ssm')
    resource = client.get_parameter(Name=name, WithDecryption=True)
    value = resource.get('Parameter').get('Value')
    return value


# get smtplib object using airflow config details
def _get_smtp_obj():
    smtp_host = conf.get('smtp', 'SMTP_HOST')
    smtp_port = conf.getint('smtp', 'SMTP_PORT')
    smtp_from = conf.get('smtp', 'SMTP_MAIL_FROM')
    smtp_pwd = conf.get('smtp', 'SMTP_PASSWORD')
    smtp_obj = smtplib.SMTP(smtp_host, smtp_port)
    smtp_obj.starttls()
    smtp_obj.login(smtp_from, smtp_pwd)
    return smtp_obj


def get_snowflake_info(ssm_client):

    logging.info('Getting env info')
    env_param = ssm_client.get_parameter(Name='env', WithDecryption=True)
    env_value = env_param.get('Parameter').get('Value')
    snf_acc_param = ssm_client.get_parameter(Name='/users/snowflake/account',
                                             WithDecryption=True)
    snf_acc_value = snf_acc_param.get('Parameter').get('Value')
    snf_user_param = ssm_client.get_parameter(
                    Name='/users/snowflake/account/user', WithDecryption=True)
    snf_user_value = snf_user_param.get('Parameter').get('Value')
    snf_key_param = ssm_client.get_parameter(
                    Name='/users/snowflake/account/password',
                    WithDecryption=True)
    snf_key_value = snf_key_param.get('Parameter').get('Value')
    env_var = {'env': env_value, 'snf_account': snf_acc_value,
               'snf_user': snf_user_value, 'snf_key': snf_key_value}
    env_var['warehouse'] = 'AWS_WH'
    return env_var


session = boto3.session.Session()
client = session.client('ssm', region_name='us-east-1')
sf_secret = get_snowflake_info(client)
params = {'snowflake_user': sf_secret['snf_user'],
          'password': sf_secret['snf_key'],
          'account': sf_secret['snf_account'],
          'warehouse': sf_secret['warehouse'],
          'snowflake_role': 'sysadmin'}
env = sf_secret['env']
session = boto3.session.Session()
client = session.client('ssm', region_name='us-east-1')

yardi_db = _get_ssm_value('/yardi/db')
yardi_user = _get_ssm_value('/yardi/user')
yardi_pwd = _get_ssm_value('/yardi/pwd')
yardi_server = _get_ssm_value('/yardi/server')

salesforce_user = _get_ssm_value('/img_reports/salesforce/username')
salesforce_pwd = _get_ssm_value('/img_reports/salesforce/password')
salesforce_token = _get_ssm_value('/img_reports/salesforce/security_token')

def get_validation_report():

    try:

        counts_query = """
        with edw_dbt_rows AS (
        SELECT table_name, TABLE_SCHEMA AS schema_name_snowflake,
        TABLE_CATALOG AS database_name_snowflake, ROW_COUNT AS rows_snowflake,
        last_altered AS last_altered_snowflake
        FROM PROD_TZ.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA='EDW'
        AND TABLE_TYPE='BASE TABLE'
        AND TABLE_NAME NOT IN ('DATABASECHANGELOG', 'DATABASECHANGELOGLOCK')
        ),

        edw_source_rows AS (
        SELECT table_name, ROW_COUNT AS rows_source
        FROM PROD_TZ.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA='EDW_DBO_REPLICA'
        AND TABLE_TYPE='BASE TABLE'
        AND TABLE_NAME NOT IN ('DATABASECHANGELOG', 'DATABASECHANGELOGLOCK')
        ),

        edw_rows AS (
        SELECT d.table_name,
        d.schema_name_snowflake,
        d.database_name_snowflake,
        d.rows_snowflake,
        r.rows_source,
        d.last_altered_snowflake 
        FROM edw_dbt_rows d
        INNER JOIN edw_source_rows r
        ON LOWER(d.table_name) = LOWER(r.table_name)
        UNION
        SELECT table_name, TABLE_SCHEMA AS schema_name_snowflake,
        TABLE_CATALOG AS database_name_snowflake, NULL AS rows_snowflake,
        ROW_COUNT AS rows_source,
        last_altered AS last_altered_snowflake
        FROM PROD_TZ.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA='EDW_DBO_REPLICA'
        AND TABLE_NAME='FACTDAILYHOMESTATUSSNAPSHOT'
        ),

        edw_dbt_columns AS (
        SELECT table_name, count(*) as columns_snowflake
        FROM PROD_TZ.information_schema.columns
        WHERE TABLE_SCHEMA='EDW'
        AND TABLE_NAME NOT IN ('DATABASECHANGELOG', 'DATABASECHANGELOGLOCK')
        GROUP BY table_name
        ),

        edw_source_columns AS (
        SELECT table_name, count(*) as columns_source
        FROM PROD_TZ.information_schema.columns
        WHERE TABLE_SCHEMA='EDW_DBO_REPLICA'
        AND TABLE_NAME NOT IN ('DATABASECHANGELOG', 'DATABASECHANGELOGLOCK')
        GROUP BY table_name
        ),

        edw_columns AS (
        SELECT d.table_name, columns_snowflake, columns_source
        FROM edw_dbt_columns d
        INNER JOIN edw_source_columns r
            ON LOWER(d.table_name) = LOWER(r.table_name)
        UNION
        SELECT table_name, NULL as columns_snowflake, count(*) as columns_source
        FROM PROD_TZ.information_schema.columns
        WHERE TABLE_SCHEMA='EDW_DBO_REPLICA'
        AND TABLE_NAME='FACTDAILYHOMESTATUSSNAPSHOT'
        GROUP BY table_name
        )

        SELECT DISTINCT
        n.TABLE_NAME AS table_view_name, 
        n.SCHEMA_NAME_SNOWFLAKE,
        n.DATABASE_NAME_SNOWFLAKE,
        n.ROWS_SNOWFLAKE AS EDW_SNOWFLAKE_ROWS,
        n.rows_source AS EDW_REPLICA_ROWS,
        ROWCOUNT AS EDW_LEGACY_ROWS,
        c.COLUMNS_SNOWFLAKE AS EDW_SNOWFLAKE_COLUMNS,
        c.COLUMNS_SOURCE AS EDW_REPLICA_COLUMNS,
        COLUMNCOUNT AS EDW_LEGACY_COLUMNS,
        n.LAST_ALTERED_SNOWFLAKE,
        CURRENT_TIMESTAMP AS insert_date,
        CASE WHEN n.LAST_ALTERED_SNOWFLAKE IS NULL THEN ''
        WHEN n.LAST_ALTERED_SNOWFLAKE > dateadd(hour, -24, CURRENT_TIMESTAMP) THEN 'Y'
        ELSE 'N' END AS daily_refresh_check,
        CASE WHEN n.rows_source IS NULL THEN ''
        WHEN (n.ROWS_SNOWFLAKE=n.rows_source AND n.rows_source=ROWCOUNT) THEN 'Y'
        ELSE 'N' END AS rows_match_check,
        CASE WHEN c.columns_source IS NULL THEN ''
        WHEN (c.COLUMNS_SNOWFLAKE=c.columns_source AND c.columns_source=COLUMNCOUNT) THEN 'Y'
        ELSE 'N' END AS columns_match_check
        FROM edw_rows n
        LEFT JOIN edw_columns c
            ON n.table_name=c.table_name
        LEFT JOIN "PROD_TZ"."EDW_DBO_REPLICA"."VWTABLESTATS" ss
            ON n.TABLE_NAME = ss.tablename
        ORDER BY n.DATABASE_NAME_SNOWFLAKE, n.SCHEMA_NAME_SNOWFLAKE, n.TABLE_NAME
        """

        nullable_type_query = """
        with edw_replica_column_stats AS (
        SELECT table_catalog AS edw_replica_table_catalog, 
        table_schema AS edw_replica_table_schema, 
        table_name AS edw_replica_table_name, 
        column_name AS edw_replica_column_name, 
        is_nullable AS edw_replica_is_nullable, 
        data_type AS edw_replica_data_type, 
        CHARACTER_MAXIMUM_LENGTH AS edw_replica_data_type_length
        FROM PROD_TZ.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'EDW_DBO_REPLICA'
        ),

        edw_column_stats AS (
        SELECT table_catalog AS edw_table_catalog, 
        table_schema AS edw_table_schema, 
        table_name AS edw_table_name, 
        column_name AS edw_column_name, 
        is_nullable AS edw_is_nullable, 
        data_type AS edw_data_type,
        CHARACTER_MAXIMUM_LENGTH AS edw_data_type_length
        FROM PROD_TZ.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'EDW'
        ),

        edw_column_stats1 AS (
        SELECT 
        edw_replica_table_catalog,
        edw_replica_table_schema,
        edw_replica_table_name,  
        edw_replica_column_name,  
        edw_replica_is_nullable,  
        edw_replica_data_type,  
        edw_replica_data_type_length,  
        edw_table_catalog,  
        edw_table_schema,  
        edw_table_name,  
        edw_column_name,  
        edw_is_nullable,  
        edw_data_type, 
        edw_data_type_length    
        FROM edw_replica_column_stats r
        LEFT JOIN edw_column_stats e
        ON edw_replica_table_name = edw_table_name
        AND edw_replica_column_name = edw_column_name
        WHERE edw_replica_is_nullable != edw_is_nullable
            OR edw_replica_data_type != edw_data_type
        ),

        create_flags AS (
        SELECT
        edw_replica_table_catalog,
        edw_replica_table_schema,
        edw_replica_table_name,  
        edw_replica_column_name,  
        edw_replica_is_nullable,  
        edw_replica_data_type,  
        edw_replica_data_type_length,  
        edw_table_catalog,  
        edw_table_schema,  
        edw_table_name,  
        edw_column_name,  
        edw_is_nullable,  
        edw_data_type, 
        edw_data_type_length,            
        CASE WHEN CONCAT(edw_replica_data_type, IFNULL(edw_replica_data_type_length, 1)) = CONCAT(edw_data_type, IFNULL(edw_data_type_length, 1)) THEN 'Y'
        WHEN RTRIM(EDW_REPLICA_COLUMN_NAME) LIKE '%WID' THEN 'Y'
        ELSE 'N' END AS data_type_length_check,
        CASE WHEN EDW_REPLICA_IS_NULLABLE=EDW_IS_NULLABLE
        THEN 'Y' ELSE 'N' END AS is_nullable_check
        FROM edw_column_stats1
        )

        SELECT
        edw_table_catalog,  
        edw_table_schema,  
        edw_table_name,  
        edw_column_name,  
        edw_is_nullable,  
        edw_data_type, 
        edw_data_type_length,
        edw_replica_is_nullable,  
        edw_replica_data_type,  
        edw_replica_data_type_length,  
        CURRENT_TIMESTAMP AS insert_date,
        data_type_length_check,
        is_nullable_check
        FROM create_flags
        ORDER BY edw_table_catalog, edw_table_schema, edw_table_name, edw_column_name
        """

        name_query = """
        with edw_replica_column_stats AS (
        SELECT table_catalog AS edw_replica_table_catalog, table_schema AS edw_replica_table_schema, table_name AS edw_replica_table_name, column_name AS edw_replica_column_name, is_nullable AS edw_replica_is_nullable, 
        data_type AS edw_replica_data_type
        FROM PROD_TZ.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'EDW_DBO_REPLICA'
        ),

        edw_column_stats AS (
        SELECT table_catalog AS edw_table_catalog, table_schema AS edw_table_schema, table_name AS edw_table_name, column_name AS edw_column_name, is_nullable AS edw_is_nullable, data_type AS edw_data_type
        FROM PROD_TZ.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'EDW'
        ),

        edw_column_stats2 AS (
        SELECT *
        FROM edw_column_stats e 
        LEFT JOIN edw_replica_column_stats r
        ON edw_replica_table_name = edw_table_name
        AND edw_replica_column_name = edw_column_name
        WHERE edw_replica_column_name IS NULL
        )

        SELECT 
        EDW_TABLE_CATALOG, 
        EDW_TABLE_SCHEMA, 
        EDW_TABLE_NAME, 
        EDW_COLUMN_NAME,
        CURRENT_TIMESTAMP AS insert_date,
        'N' AS column_name_check
        FROM edw_column_stats2
        WHERE edw_table_name IN
        (SELECT table_name
        FROM PROD_TZ.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'EDW_DBO_REPLICA')
        ORDER BY EDW_TABLE_CATALOG, EDW_TABLE_SCHEMA, EDW_REPLICA_TABLE_NAME, EDW_COLUMN_NAME
        """

        yardi_sf_trans_query = """
        with row_altered_cte AS (

        SELECT table_name, 
        TABLE_SCHEMA AS schema_name_snowflake,
        TABLE_CATALOG AS database_name_snowflake,
        ROW_COUNT AS rows_snowflake,
        last_altered AS last_altered_snowflake
        FROM PROD_RZ.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA='YARDI_DBO'
        AND TABLE_TYPE='BASE TABLE'
        AND TABLE_NAME NOT IN ('DATABASECHANGELOG', 'DATABASECHANGELOGLOCK')

        UNION

        SELECT table_name, 
        TABLE_SCHEMA AS schema_name_snowflake,
        TABLE_CATALOG AS database_name_snowflake,
        ROW_COUNT AS rows_snowflake,
        last_altered AS last_altered_snowflake
        FROM MULESOFT_PROD.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA='TRANSUNION'
        AND TABLE_TYPE='BASE TABLE'
        AND TABLE_NAME NOT IN ('DATABASECHANGELOG', 'DATABASECHANGELOGLOCK')

        UNION

        SELECT table_name, 
        TABLE_SCHEMA AS schema_name_snowflake,
        TABLE_CATALOG AS database_name_snowflake,
        ROW_COUNT AS rows_snowflake,
        last_altered AS last_altered_snowflake
        FROM PROD_RZ.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA='INVH_SALESFORCE'
        AND TABLE_TYPE='BASE TABLE'
        AND TABLE_NAME NOT IN ('DATABASECHANGELOG', 'DATABASECHANGELOGLOCK')
        ),

        columns_cte AS (

        SELECT table_name, count(*) as columns_snowflake
        FROM PROD_RZ.information_schema.columns
        WHERE TABLE_NAME NOT IN ('DATABASECHANGELOG', 'DATABASECHANGELOGLOCK')
        GROUP BY table_name

        UNION

        SELECT table_name, count(*) as columns_snowflake
        FROM MULESOFT_PROD.information_schema.columns
        WHERE TABLE_NAME NOT IN ('DATABASECHANGELOG', 'DATABASECHANGELOGLOCK')
        GROUP BY table_name

        UNION

        SELECT table_name, count(*) as columns_snowflake
        FROM PROD_RZ.information_schema.columns
        WHERE TABLE_NAME NOT IN ('DATABASECHANGELOG', 'DATABASECHANGELOGLOCK')
        GROUP BY table_name
        )

        SELECT
        n.TABLE_NAME AS table_name,
        n.SCHEMA_NAME_SNOWFLAKE,
        n.DATABASE_NAME_SNOWFLAKE,
        n.ROWS_SNOWFLAKE AS SNOWFLAKE_ROWS,
        c.COLUMNS_SNOWFLAKE AS SNOWFLAKE_COLUMNS,
        n.LAST_ALTERED_SNOWFLAKE,
        CURRENT_TIMESTAMP AS insert_date
        FROM row_altered_cte n
        LEFT JOIN columns_cte c
            ON n.table_name=c.table_name
        ORDER BY n.DATABASE_NAME_SNOWFLAKE, n.SCHEMA_NAME_SNOWFLAKE, n.TABLE_NAME
        """

        engine = create_engine(URL(
            user=sf_secret['snf_user'],
            password=sf_secret['snf_key'],
            account=sf_secret['snf_account'],
            warehouse=sf_secret['warehouse'],
            role='sysadmin',
        ))
        connection = engine.connect()

        # run queries
        df_counts = pd.read_sql(counts_query, connection)
        df_counts.columns = [x.lower() for x in df_counts.columns]

        df_nullable_type = pd.read_sql(nullable_type_query, connection)
        df_nullable_type.columns = [x.lower() for x in df_nullable_type.columns]

        df_name = pd.read_sql(name_query, connection)
        df_name.columns = [x.lower() for x in df_name.columns]

        df_yardi_sf_trans = pd.read_sql(yardi_sf_trans_query, connection)
        df_yardi_sf_trans.columns = [x.lower() for x in df_yardi_sf_trans.columns]

        connection.close()
        engine.dispose()
        return df_counts, df_nullable_type, df_name, df_yardi_sf_trans

    except Exception as e:
        print(e)

def yardi_source_report(df_yardi_sf_trans, yardi_server, yardi_user, yardi_pwd, yardi_db):

    yardi_tables = tuple([x.upper() for x in list(df_yardi_sf_trans[df_yardi_sf_trans['schema_name_snowflake'] == 'YARDI_DBO']['table_name'])])

    try:
        # create yardi connection
        # engine = create_engine('mssql+pyodbc://{}:{}@{}/{}?driver=SQL+Server+Native+Client+11.0'.format(yardi_user, yardi_pwd, yardi_server, yardi_db))
        engine = create_engine('mssql+pymssql://{}:{}@{}/{}?'.format(yardi_user, yardi_pwd, yardi_server, yardi_db))

        yardi_query = """
        with yardi_rows AS (
        SELECT upper(OBJECT_NAME(p.object_id)) AS table_name
        ,SUM(p.rows) AS rows_yardi
        FROM sys.partitions p
        INNER JOIN sys.tables t ON p.object_id = t.object_id
        WHERE p.index_id < 2
        AND upper(OBJECT_NAME(p.object_id)) IN {yardi_tables}
        GROUP BY p.object_id
        ,t.schema_id
        
        UNION

        SELECT 'BOOKS' as table_name, COUNT(*) AS rows_yardi
        FROM {db}.dbo.books 

        UNION 

        SELECT 'PROPATTRIBUTES' as table_name, COUNT(*) AS rows_yardi
        FROM {db}.dbo.propattributes

        UNION 

        SELECT 'TENSTATUS' as table_name, COUNT(*) AS rows_yardi
        FROM {db}.dbo.tenstatus        

        ),

        yardi_columns AS (
        SELECT upper(table_name) AS table_name, 
        count(*) as columns_yardi
        FROM {db}.information_schema.columns [NO LOCK]
        WHERE upper(table_name) IN {yardi_tables}
        GROUP BY table_name
        )
                    
        SELECT t.table_name, rows_yardi AS source_rows, columns_yardi AS source_columns
        FROM yardi_rows t
        LEFT JOIN yardi_columns r
        ON t.table_name = r.table_name
        """.format(db=yardi_db, yardi_tables=yardi_tables)

        df_yardi_source = pd.read_sql(yardi_query, con=engine)
        df_yardi_source.columns = [x.lower() for x in df_yardi_source.columns]

        return df_yardi_source

    except Exception as e:
        print(e)
        logging.error(str(datetime.now()) + ': ' + str(e))    


def salesforce_source_report(df_yardi_sf_trans, salesforce_user, salesforce_pwd, salesforce_token):

    salesforce_list = list([x for x in list(df_yardi_sf_trans[df_yardi_sf_trans['schema_name_snowflake'] == 'INVH_SALESFORCE']['table_name'])])
    list_objects = []

    sf = Salesforce(username=salesforce_user, 
                    password=salesforce_pwd, 
                    security_token=salesforce_token, 
                    domain='login'
                    )   

    for x in salesforce_list:

        try:        

            row_count = sf.query("SELECT count() FROM {}".format(x))
            row_count = row_count['totalSize']

            desc = getattr(sf, x).describe()
            column_count = len([field['name'] for field in desc['fields']])

            list_objects.append([x, row_count, column_count])

        except Exception as e:
            print(e)
            continue

    df_salesforce_source = pd.DataFrame(list_objects, columns=['table_name', 'source_rows', 'source_columns'])
    df_salesforce_source['table_name'] = df_salesforce_source['table_name'].apply(lambda x: x.upper())

    return df_salesforce_source

# def highlight_rows(df):

#     if df.daily_refresh_check == 'N' or df.columns_match_check == 'N' or df.rows_match_check == 'N':
#         return ['background-color: yellow'] * len(df)
#     else:
#         return ['background-color: white'] * len(df)


def create_indicators(df):

    try:

        # create flag for date_altered (must be within last 24 hrs)
        df['daily_refresh_check'] = ['' if pd.isna(i) else 'Y' if i > (datetime.utcnow().replace(tzinfo=pytz.utc) - timedelta(hours=24)) else 'N' for i in df['last_altered_snowflake']] 

        # create flag for column match (exact columns count has to match)
        df['columns_match_check'] = ['' if pd.isna(i) else 'Y' if i==j else 'N' for i,j in zip(df['source_columns'],df['snowflake_columns'])]

        # create flag for row match (exact row count has to match)
        df['rows_match_check'] = ['' if pd.isna(i) else 'Y' if i==j else 'N' for i,j in zip(df['source_rows'],df['snowflake_rows'])]

        return df

    except Exception as e:
        print(e)

def format_df(df_counts, df_nullable_type, df_name, df_yardi_sf_trans):

    df_counts['last_altered_snowflake'] = pd.to_datetime(df_counts['last_altered_snowflake'])
    df_counts['edw_snowflake_rows'] = df_counts['edw_snowflake_rows'].astype(float).astype('Int64')
    df_counts['edw_replica_rows'] = df_counts['edw_replica_rows'].astype(float).astype('Int64')
    df_counts['edw_legacy_rows'] = df_counts['edw_legacy_rows'].astype(float).astype('Int64')
    df_counts['edw_snowflake_columns'] = df_counts['edw_snowflake_columns'].astype(float).astype('Int64')
    df_counts['edw_replica_columns'] = df_counts['edw_replica_columns'].astype(float).astype('Int64')
    df_counts['edw_legacy_columns'] = df_counts['edw_legacy_columns'].astype(float).astype('Int64')

    df_nullable_type['edw_replica_data_type_length'] = df_nullable_type['edw_replica_data_type_length'].astype(float).astype('Int64')
    df_nullable_type['edw_data_type_length'] = df_nullable_type['edw_data_type_length'].astype(float).astype('Int64')
    
    df_yardi_sf_trans['last_altered_snowflake'] = pd.to_datetime(df_yardi_sf_trans['last_altered_snowflake'])
    df_yardi_sf_trans['snowflake_rows'] = df_yardi_sf_trans['snowflake_rows'].astype(float).astype('Int64')
    df_yardi_sf_trans['source_rows'] = df_yardi_sf_trans['source_rows'].astype(float).astype('Int64')
    df_yardi_sf_trans['snowflake_columns'] = df_yardi_sf_trans['snowflake_columns'].astype(float).astype('Int64')
    df_yardi_sf_trans['source_columns'] = df_yardi_sf_trans['source_columns'].astype(float).astype('Int64')

    return df_counts, df_nullable_type, df_name, df_yardi_sf_trans


def write_to_snowflake(df_counts, df_nullable_type, df_name, df_yardi_sf_trans):

    snowflake_conn = create_engine(URL(
    user=sf_secret['snf_user'],
    password=sf_secret['snf_key'],
    account=sf_secret['snf_account'],
    warehouse=sf_secret['warehouse'],
    database='DEV_TZ',
    schema='PUBLIC',
    role='sysadmin'
    ))

    print('Connected..')
    
    try:
        
        df_counts.to_sql(name='edw_validation_counts', con=snowflake_conn, index=False, if_exists='append')
        df_nullable_type.to_sql(name='edw_validation_nullable_types', con=snowflake_conn, index=False, if_exists='append')
        df_name.to_sql(name='edw_validation_column_names', con=snowflake_conn, index=False, if_exists='append')
        df_yardi_sf_trans.to_sql(name='edw_validation_yardi_sf_trans', con=snowflake_conn, index=False, if_exists='append')

        print('All tables written to Snowflake successfully...')

    except Exception as e:
        print(e)


def apply_style(df_counts, df_nullable_type, df_name, df_yardi_sf_trans):

    # style df
    df_counts_f = df_counts.reset_index(drop=True).style.set_properties(
        **{'text-align': 'left', 'border': '1.3px solid black'})
    df_counts_f = df_counts_f.hide_index()

    df_nullable_type_f = df_nullable_type.reset_index(drop=True).style.set_properties(
        **{'text-align': 'left', 'border': '1.3px solid black'})
    df_nullable_type_f = df_nullable_type_f.hide_index()

    df_name_f = df_name.reset_index(drop=True).style.set_properties(
        **{'text-align': 'left', 'border': '1.3px solid black'})
    df_name_f = df_name_f.hide_index()

    df_yardi_sf_trans_f = df_yardi_sf_trans.reset_index(drop=True).style.set_properties(
        **{'text-align': 'left', 'border': '1.3px solid black'})
    df_yardi_sf_trans_f = df_yardi_sf_trans_f.hide_index()

    return df_counts_f, df_nullable_type_f, df_name_f, df_yardi_sf_trans_f


def send_email():

    try:

        # get data
        df_counts, df_nullable_type, df_name, df_yardi_sf_trans = get_validation_report()
        print(df_yardi_sf_trans.head())
        print('df_counts, df_nullable_type, df_name, df_yardi_sf_trans SUCCESS...')

    except Exception as e:
        print(e)

    try:

        # get yardi source data
        df_yardi_source = yardi_source_report(df_yardi_sf_trans, yardi_server, yardi_user, yardi_pwd, yardi_db)
        print('df_yardi_source SUCCESS...')

    except Exception as e:
        print(e)

    try:

        # get salesforce source data
        df_salesforce_source = salesforce_source_report(df_yardi_sf_trans, salesforce_user, salesforce_pwd, salesforce_token)
        print(df_salesforce_source.head())
        print('df_salesforce_source SUCCESS...')

    except Exception as e:
        print(e)

    try:

        # mergi yardi edw vs. yardi source data, create indicators
        df_yardi = df_yardi_sf_trans[(df_yardi_sf_trans['schema_name_snowflake']=='YARDI_DBO')]
        df_yardi = df_yardi.merge(df_yardi_source, how='left', on='table_name')
        print(df_yardi.head())
        print('merge 1 success...')
        df_salesforce = df_yardi_sf_trans[(df_yardi_sf_trans['schema_name_snowflake']=='INVH_SALESFORCE')]
        df_salesforce = df_salesforce.merge(df_salesforce_source, how='left', on='table_name')
        print(df_salesforce.head())
        print('merge 2 success...')
        df_yardi_salesforce = pd.concat([df_yardi, df_salesforce])
        df_trans = df_yardi_sf_trans[(df_yardi_sf_trans['schema_name_snowflake']=='TRANSUNION')]
        df_trans['source_rows'], df_trans['source_columns'] = np.nan, np.nan
        df_yardi_sf_trans = pd.concat([df_yardi_salesforce, df_trans])
        print(df_yardi_sf_trans.head())
        print('merge 3 success...')
        df_yardi_sf_trans = create_indicators(df_yardi_sf_trans)
        print('create indicators success...')
        df_yardi_sf_trans = df_yardi_sf_trans[['table_name', 'schema_name_snowflake', 'database_name_snowflake', 'snowflake_rows', 'snowflake_columns', 'source_rows', 'source_columns', 'last_altered_snowflake', 'insert_date', 'daily_refresh_check', 'columns_match_check', 'rows_match_check']]
        print(df_yardi_sf_trans.head())
        print('df_yardi_sf_trans merge SUCCESS...')

    except Exception as e:
        print(e)

    try:
        # format df
        df_counts, df_nullable_type, df_name, df_yardi_sf_trans = format_df(df_counts, df_nullable_type, df_name, df_yardi_sf_trans)
        print('df_counts, df_nullable_type, df_name, df_yardi_sf_trans format SUCCESS...')

    except Exception as e:
        print(e)

    try:
        write_to_snowflake(df_counts, df_nullable_type, df_name, df_yardi_sf_trans)
        print('write_to_snowflake SUCCESS...')

    except Exception as e:
        print(e)    

    try:

        # filter for only failed checks
        # mask = (df_counts['daily_refresh_check'] =='N') | (df_counts['columns_match_check'] =='N') | (df_counts['rows_match_check'] =='N')
        # df_counts = df_counts[mask]

        mask = (df_nullable_type['data_type_length_check'] =='N') | (df_nullable_type['is_nullable_check'] =='N') 
        df_nullable_type = df_nullable_type[mask]

        mask = (df_name['column_name_check'] =='N')
        df_name = df_name[mask]

        mask = (df_yardi_sf_trans['daily_refresh_check'] =='N') | (df_yardi_sf_trans['columns_match_check'] =='N') | (df_yardi_sf_trans['rows_match_check'] =='N') | (df_yardi_sf_trans['columns_match_check'] =='') | (df_yardi_sf_trans['rows_match_check'] =='')
        df_yardi_sf_trans = df_yardi_sf_trans[mask]
        print('masking SUCCESS...')

    except Exception as e:
        print(e)

    try:
        # apply styles to failed checks
        df_counts_f, df_nullable_type_f, df_name_f, df_yardi_sf_trans_f = apply_style(df_counts, df_nullable_type, df_name, df_yardi_sf_trans)
        print('df_counts_f, df_nullable_type_f, df_name_f, df_yardi_sf_trans_f apply_style SUCCESS...')

    except Exception as e:
        print(e)

    try:

        # send email
        date_today = date.today()
        obj = _get_smtp_obj()
        smtp_from = conf.get('smtp', 'SMTP_MAIL_FROM')
        recipients = ['EDA-DataFactory@invitationhomes.com', 'EDA-EDW@invitationhomes.com', 'EDA-Leadership@invitationhomes.com', 'jeff.swerens@invitationhomes.com']

        # create MIME object
        msg = MIMEMultipart('alternative')

        smtp_body = """\
        <html>
        <head></head>
        <body>
        <h3>Snowflake validation report</h3>
        <br>
        <h5>Note 1: This is an automated email. See link to Github for more info:
        https://github.com/invitation-homes/datalake-airflow-jobs/blob/DEDW-2990-DAILY-VALIDATION-ALERTS-V2/DAG/daily_validation_alerts_new.py</h5>
        <h5>Note 2: All tables below are written to Snowflake here: DEV_TZ.PUBLIC</h5>
        <br>
        <h4> edw_validation_counts:</h4>
        <br>
        {}
        <br>
        <h4> edw_validation_nullable_types:</h4>
        <br>
        {}
        <br>
        <h4> edw_validation_column_names:</h4>
        <br>
        {}
        <br>
        <h4> edw_validation_yardi_sf_trans:</h4>
        <br>
        {}
        <br>
        </body>
        </html>
        """.format(df_counts_f.render(), df_nullable_type_f.render(), df_name_f.render(), df_yardi_sf_trans_f.render())

        msg['Subject'] = f"Daily Snowflake Validation Report (PROD) - {date_today}"
        msg['X-Priority'] = '2'
        msg['From'] = smtp_from
        msg['To'] = ", ".join(recipients)
        mime_body = MIMEText(smtp_body, 'html')
        msg.attach(mime_body)

        # send mail and close smtplib object connection
        obj.sendmail(to_addrs=recipients, from_addr=smtp_from, msg=msg.as_string())
        obj.quit()
        print('send_email SUCCESS...')

    except Exception as e:
        print(e)


start_validation = DummyOperator(
    task_id='start_validation',
    dag=dag
)

validation_email = PythonOperator(
    task_id='daily_validation_email',
    python_callable=send_email,
    provide_context=True,
    dag=dag
)

end_validation = DummyOperator(
    task_id='end_validation',
    dag=dag
)

start_validation >> validation_email >> end_validation