

# get the snowflake authentication information
def get_snowflake_info(ssm_client):

    logging.info('Getting env info')
    env_param = ssm_client.get_parameter(Name='env', WithDecryption=True)
    env_value = env_param.get('Parameter').get('Value')

    snf_acc_param = ssm_client.get_parameter(
        Name='/users/snowflake/account', WithDecryption=True)
    snf_acc_value = snf_acc_param.get('Parameter').get('Value')

    snf_user_param = ssm_client.get_parameter(
        Name='/users/snowflake/account/user', WithDecryption=True)
    snf_user_value = snf_user_param.get('Parameter').get('Value')

    snf_key_param = ssm_client.get_parameter(
        Name='/users/snowflake/account/password', WithDecryption=True)
    snf_key_value = snf_key_param.get('Parameter').get('Value')
    env_var = {'env': env_value, 'snf_account': snf_acc_value,
               'snf_user': snf_user_value, 'snf_key': snf_key_value}

    env_var['warehouse'] = 'AWS_WH'
    env_var['schema'] = 'NATIONAL_RTM'

    if (env_value == 'qa'):
        env_var['database'] = 'QA_RZ'
    elif (env_value == 'prod'):
        env_var['database'] = 'PROD_RZ'
    else:
        env_var['database'] = 'DEV_RZ'
    return env_var


session = boto3.session.Session()
client = session.client('ssm', region_name='us-east-1')
# client = boto3.client('ssm', region_name='us-east-1')
sf_secret = get_snowflake_info(client)
params = {'snowflake_user': sf_secret['snf_user'],
          'password': sf_secret['snf_key'],
          'account': sf_secret['snf_account'],
          'warehouse': sf_secret['warehouse'],
          'database': sf_secret['database'],
          'schema': sf_secret['schema'],
          'snowflake_role': 'sysadmin'
          }
env = sf_secret['env']


# loading data of device API from S3 to Snowflake GEOTAB_DEVICE table

def load_geotab_device_data():
    try:
        task_id = 'load_device_data_task'
        snowflake_cur = SnowflakeCursor(params)
        s = snowflake_audit()
        s.start_audit(snowflake_cur, 'GEOTAB_DEVICE', sf_secret['schema'],
                      dag.dag_id, task_id)
        snowflake_cur.execute(
            "copy into " + sf_secret['database'] + "." + sf_secret['schema'] +
            ".GEOTAB_DEVICE from " +
            "(select * ,CURRENT_TIMESTAMP(),$1:id::varchar from @" +
            sf_secret['database'] + "." + sf_secret['schema'] +
            ".STG_GEOTAB/DEVICE) FILE_FORMAT = ( FORMAT_NAME =  " +
            sf_secret['database'] + "." + sf_secret['schema'] +
            ".GEOTAB_JSON_RAWDATA);")
        res = snowflake_cur.fetchall()
        s.end_success_audit(res)
        snowflake_cur.close()
    except Exception as err:
        print(err)
        s.end_failure_audit(err)
        raise (err)

# loading data of trip API from S3 to Snowflake GEOTAB_TRIPS table


def load_geotab_trip_data():
    try:
        task_id = 'load_trip_data_task'
        snowflake_cur = SnowflakeCursor(params)
        s = snowflake_audit()
        s.start_audit(snowflake_cur, 'GEOTAB_TRIPS', sf_secret['schema'],
                      dag.dag_id, task_id)
        snowflake_cur.execute(
            "copy into " + sf_secret['database'] + "." + sf_secret['schema'] +
            ".GEOTAB_TRIPS from " +
            "(select * ,CURRENT_TIMESTAMP(),$1:id::varchar,"
            + " $1:device.id::varchar from @"
            + sf_secret['database'] + "." + sf_secret['schema'] +
            ".STG_GEOTAB/TRIPS) FILE_FORMAT = ( FORMAT_NAME =  " +
            sf_secret['database'] + "." + sf_secret['schema'] +
            ".GEOTAB_JSON_RAWDATA);")
        res = snowflake_cur.fetchall()
        s.end_success_audit(res)
        snowflake_cur.close()
    except Exception as err:
        print(err)
        s.end_failure_audit(err)
        raise (err)


# loading data of exceptionEvent API from
# S3 to Snowflake GEOTAB_EXCEPTIONEVENT table
def load_geotab_exceptionEvent_data():
    try:
        task_id = 'load_exceptionEvent_data_task'
        snowflake_cur = SnowflakeCursor(params)
        s = snowflake_audit()
        s.start_audit(snowflake_cur, 'GEOTAB_EXCEPTIONEVENT',
                      sf_secret['schema'], dag.dag_id, task_id)
        snowflake_cur.execute(
            "copy into " + sf_secret['database'] + "." + sf_secret['schema'] +
            ".GEOTAB_EXCEPTIONEVENT from " +
            "(select * ,CURRENT_TIMESTAMP(),$1:id::varchar from @" +
            sf_secret['database'] + "." + sf_secret['schema'] +
            ".STG_GEOTAB/ExceptionEvent) FILE_FORMAT = ( FORMAT_NAME =  " +
            sf_secret['database'] + "." + sf_secret['schema'] +
            ".GEOTAB_JSON_RAWDATA);")
        res = snowflake_cur.fetchall()
        s.end_success_audit(res)
        snowflake_cur.close()
    except Exception as err:
        print(err)
        s.end_failure_audit(err)
        raise (err)


# Loading data of Group API from S3 to GEOTAB_GROUP table
def load_geotab_group_data():
    try:
        task_id = 'load_group_data_task'
        snowflake_cur = SnowflakeCursor(params)
        s = snowflake_audit()
        s.start_audit(snowflake_cur, 'GEOTAB_GROUP', sf_secret['schema'],
                      dag.dag_id, task_id)
        sql1 = "delete from "+sf_secret['database'] + \
            "."+sf_secret['schema']+".GEOTAB_GROUP"
        snowflake_cur.execute(sql1)
        snowflake_cur.execute(
            "copy into " + sf_secret['database'] + "." + sf_secret['schema'] +
            ".GEOTAB_GROUP from " +
            "(select * ,CURRENT_TIMESTAMP(),$1:id::varchar from @" +
            sf_secret['database'] + "." + sf_secret['schema'] +
            ".STG_GEOTAB/GROUP) FILE_FORMAT = ( FORMAT_NAME =  " +
            sf_secret['database'] + "." + sf_secret['schema'] +
            ".GEOTAB_JSON_RAWDATA);")
        res = snowflake_cur.fetchall()
        s.end_success_audit(res)
        snowflake_cur.close()
    except Exception as err:
        print(err)
        s.end_failure_audit(err)
        raise (err)


# Loading data of zonetype API from S3 to GEOTAB_ZONETYPE table
def load_geotab_zonetype_data():
    try:
        task_id = 'load_zonetype_data_task'
        snowflake_cur = SnowflakeCursor(params)
        s = snowflake_audit()
        s.start_audit(snowflake_cur, 'GEOTAB_ZONETYPE', sf_secret['schema'],
                      dag.dag_id, task_id)
        sql1 = "delete from "+sf_secret['database'] + \
            "."+sf_secret['schema']+".GEOTAB_ZONETYPE"
        snowflake_cur.execute(sql1)
        snowflake_cur.execute(
            "copy into " + sf_secret['database'] + "." + sf_secret['schema'] +
            ".GEOTAB_ZONETYPE from (select * ,CURRENT_TIMESTAMP()"
            + ",$1:id::varchar from @"
            + sf_secret['database'] + "." + sf_secret['schema'] +
            ".STG_GEOTAB/ZONETYPE) FILE_FORMAT = ( FORMAT_NAME =  " +
            sf_secret['database'] + "." + sf_secret['schema'] +
            ".GEOTAB_JSON_RAWDATA);")
        res = snowflake_cur.fetchall()
        s.end_success_audit(res)
        snowflake_cur.close()
    except Exception as err:
        print(err)
        s.end_failure_audit(err)
        raise (err)


# Loading data of Address API from S3 to GEOTAB_TRIP_ADDRESS table
def load_geotab_TripAddress_data():
    try:
        task_id = 'load_TripAddress_data_task'
        snowflake_cur = SnowflakeCursor(params)
        s = snowflake_audit()
        s.start_audit(snowflake_cur, 'GEOTAB_TRIP_ADDRESS',
                      sf_secret['schema'], dag.dag_id, task_id)
        snowflake_cur.execute(
            "copy into "+sf_secret['database']+"."+sf_secret['schema']
            + ".GEOTAB_TRIP_ADDRESS from "
            + "(select t.$1 ,t.$2 ,t.$3 ,t.$4 , Current_TIMESTAMP() from @"
            + sf_secret['database']+"."+sf_secret['schema']
            + ".STG_GEOTAB/address t) FILE_FORMAT = ( FORMAT_NAME =  "
            + sf_secret['database']+"."+sf_secret['schema']+".GEOTAB_CSV);"
        )
        res = snowflake_cur.fetchall()
        s.end_success_audit(res)
        snowflake_cur.close()
    except Exception as err:
        print(err)
        s.end_failure_audit(err)
        raise (err)


session = boto3.session.Session()
client = session.client('ssm', region_name='us-east-1')
secret = get_snowflake_info(client)
env = secret['env']


# Glue Job details
geotab_glue_job_name = f"datalake-geotab-api-{env}-invh"
trip_address_glue_job_name = f"datalake-geotab-trip-address-{env}-invh"


# calling main raw table glue job
geotab_glue_job_task = AwsGlueJobOperator(
    task_id='geotab_glue_job_task',
    job_name=geotab_glue_job_name,
    script_args={
        "--TASK_ID": 'geotab_glue_job_task',
        "--DAG_ID": dag.dag_id
    },
    dag=dag, on_failure_callback=email_alert
)


# Calling TripAddress Glue Job
trip_address_glue_job_task = AwsGlueJobOperator(
    task_id='trip_address_glue_job_task',
    job_name=trip_address_glue_job_name,
    script_args={
        "--TASK_ID": 'trip_address_glue_job_task',
        "--DAG_ID": dag.dag_id
    },
    dag=dag, on_failure_callback=email_alert
)

# All python DAG for calling above python funtion
# which used for loading data from S3 to Snowflake
# Dag used for loading Device table
load_device_data_task = PythonOperator(
    task_id='load_device_data_task',
    python_callable=load_geotab_device_data,
    dag=dag, on_failure_callback=email_alert
)

# Dag used for loading DEOTAB_TRIP table
load_trip_data_task = PythonOperator(
    task_id='load_trip_data_task',
    python_callable=load_geotab_trip_data,
    dag=dag, on_failure_callback=email_alert
)
# Dag used for loading DEOTAB_exceptionEvent table
load_exceptionEvent_data_task = PythonOperator(
    task_id='load_exceptionEvent_data_task',
    python_callable=load_geotab_exceptionEvent_data,
    dag=dag, on_failure_callback=email_alert
)

# Dag used for loading DEOTAB_GROUP table
load_group_data_task = PythonOperator(
    task_id='load_group_data_task',
    python_callable=load_geotab_group_data,
    dag=dag, on_failure_callback=email_alert
)


# Dag used for loading DEOTAB_ZONETYPE table
load_zonetype_data_task = PythonOperator(
    task_id='load_zonetype_data_task',
    python_callable=load_geotab_zonetype_data,
    dag=dag, on_failure_callback=email_alert
)

# Dag used for loading DEOTAB_TripAddress table
load_TripAddress_data_task = PythonOperator(
    task_id='load_TripAddress_data_task',
    python_callable=load_geotab_TripAddress_data,
    dag=dag, on_failure_callback=email_alert
)


# DAG Calling
# first it will load data into raw table
geotab_glue_job_task >> [
    load_device_data_task, load_exceptionEvent_data_task, load_group_data_task,
    load_zonetype_data_task]
# once GEOTAB_TRIP populated it will load TRIP_ADDRESS table
geotab_glue_job_task >> load_trip_data_task
load_trip_data_task >> trip_address_glue_job_task
trip_address_glue_job_task >> load_TripAddress_data_task