'''Module to use the same code for different tables in datalake-yardi-etl-load'''

import boto3
import logging
import time

dag = DAG(
    'datalake-yardi-etl-load',
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
    concurrency=20,
    schedule_interval=schedule,
    on_failure_callback=email_alert
)

def load_table(table_name):
    get_params = truncate_table(table_name)
    env = get_params[0]
    JOB_RUN = get_params[1]
    if JOB_RUN == 'Y':
        client = boto3.client('glue', region_name='us-east-1')
        gluejobname = f"datalake-yardi-{table_name.lower()}-{env}-invh"

        try:
            runId = client.start_job_run(JobName=gluejobname, Arguments={
                                         '--DAG_ID': dag.dag_id,
                                         '--TASK_ID': f'load_{table_name.lower()}_table'})

            get_job_info = client.get_job_run(
                JobName=gluejobname, RunId=runId['JobRunId'])
            status = get_job_info['JobRun']['JobRunState']
            print(
                "Job Status : ",
                get_job_info['JobRun']['JobRunState'])

            while (status not in ['STOPPED', 'FAILED', 'SUCCEEDED']):
                time.sleep(30)
                print(status)
                get_job_info = client.get_job_run(
                    JobName=gluejobname, RunId=runId['JobRunId'])
                status = get_job_info['JobRun']['JobRunState']

                if (status == 'SUCCEEDED'):
                    logging.info('Glue task SUCCEEDED')

                if (status in ['FAILED', 'STOPPED', 'TIMEOUT']):
                    get_job_info = client.get_job_run(
                        JobName=gluejobname, RunId=runId['JobRunId'])
                    failure_reason = get_job_info['JobRun'][
                        'ErrorMessage']
                    raise ValueError(
                        'Glue task failed, due to: {}'.format(
                            failure_reason))

            logging.info(f'{table_name.upper()} table loaded')
        except Exception as e:
            print(e)
            raise (e)

'''To call this function passing the table name as an argument, we can use this code'''
load_table('Table_Name')

