from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from datetime import datetime, timedelta
import logging
from datetime import date as d
import pandas as pd
from time import sleep

from sqlalchemy import create_engine

log = logging.getLogger(__name__)

# relative import from upper sub directory
import sys
import os
sys.path.insert(0, os.getcwd())
from helperutil import file_date_util as util
from helperutil.db_util import dbUtil
# from helperutil.business_util as butil
from helperutil.business_util import * # get_business_list_in_dataframe, initialize_transaction_table_dim_business

# setup default arguments
default_args = {
    'owner': 'airflow',
#    'depends_on_past': False,
    'start_date': datetime(2018, 8, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    'path_name': '/opt/app/python/app/restaurant_recommendation/data',
    'pattern_name': 'restaurant_data*json',
    'download_url': 'https://data.cityofnewyork.us/resource/9w7m-hzhe.json?inspection_date=',
    'download_date_days_ago': -3,
    'download_date_months_ago': -4,
    'download_date_incremental': True,
    'db_hostname': 'servername',
    'db_username': 'user',
    'db_password': 'user',
    'db_database': 'restaurant_db',
    'overwrite_file': True,
}


engine = create_engine("mysql+pymysql://" + default_args['db_username'] + ":" + default_args['db_password'] + "@" + default_args['db_hostname'] + "/" + default_args['db_database'] + "", echo=False)
db = dbUtil(default_args['db_hostname'], default_args['db_username'], default_args['db_password'], default_args['db_database'], True)

# generate date list for next task
def generate_download_datelist():

    path_name = default_args['path_name']
    pattern_name = default_args['pattern_name']

    date_end = util.getAddDays(d.today(), default_args['download_date_days_ago'], 'days')
    # log.debug(date_end)
    #
    # if default_args['download_date_incremental'] == True:
    #     date_start = date_end
    # else:

    date_start = util.getAddDays(date_end, default_args['download_date_months_ago'], 'month')
    # log.debug(date_start)

    downloaded_file_list = util.getdownloadedNthFiles(path_name, pattern_name)
    # log.debug(downloaded_file_list)

    downloaded_files = [i.replace(path_name + '/', '') for i in downloaded_file_list]
    # log.debug('downloaded files: ', downloaded_files)

    files_of_date_range = util.getDateRange(pattern_name.split('*')[0] + '_', date_start, date_end, '.json')
    files_of_date_range = [i.replace('-', '_') for i in files_of_date_range]
    # log.debug('files_of_date_range:', files_of_date_range)

    # log.debug(sorted(getDateRange(date_start, date_end), reverse=True))

    downloading_list = set(files_of_date_range) - set(downloaded_files)
    # log.info('downloading_list')
    # log.info(sorted(downloading_list))

    #datelist_to_download = downloading_list
    #log.info(datelist_to_download)
    return downloading_list


def download_for_this_date(**context):
    sleep(1)
    p_date = context.get('params').get('p_date')
    # print("datelist_to_download: " + datelist_to_download)
    # log.info("parameter: " + str(p_date))
    date_part = str(p_date).replace('restaurant_data_', '').replace('.json', '').replace('_', '-')
    # log.info("parameter: " + date_part)
    url = default_args['download_url'] + date_part
    json_content = pd.read_json(url).to_json(orient='records', lines=True)
    if default_args['overwrite_file']:
        if os.path.exists(default_args['path_name'] + '/' + p_date):
            os.remove(default_args['path_name'] + '/' + p_date)
    with open(default_args['path_name'] + '/' + p_date, 'w') as raw_file:
        raw_file.write(json_content)
        raw_file.close()

    return 'ok'


def fetch_and_return_download_datelist(**context):
    # log.info("datelist_to_download: " + str(datelist_to_download))
    # datelist = context['task_instance'].xcom_pull(task_ids='generate_date_list_to_process1')
    # log.info('datelist')
    # log.info(datelist)
    return 'ok'
    # return PythonOperator(task_id=)


def sub_download_dag(parent_dag_name, child_dag_name, start_date, download_file_list):
    dag_id = f'{parent_dag_name}.{child_dag_name}'
    # log.info('sub dag_id: ' + dag_id)
    mydag = DAG(
    dag_id=dag_id,
    start_date=start_date,
    )

    for i in download_file_list:
        log.info("today's downloading files: " + i)
        PythonOperator(
        task_id=f'download_{i}',
        python_callable=download_for_this_date,
        params={'p_date': i},
        provide_context=True,
        dag=mydag
        )
    return mydag


def get_process_file_list():
    path_name = default_args['path_name']
    pattern_name = default_args['pattern_name']

    date_end = util.getAddDays(d.today(), default_args['download_date_days_ago'], 'days')
    # log.info(date_end)

    date_start = util.getAddDays(date_end, default_args['download_date_months_ago'], 'month')
    # log.info(date_start)

    downloaded_file_list = util.getdownloadedNthFiles(path_name, pattern_name)

    # Get files to process from Default Date Range
    files_of_date_range = util.getDateRange(pattern_name.split('*')[0] + '_', date_start, date_end, '.json')
    files_of_date_range = [i.replace('-', '_') for i in files_of_date_range]

    # From downloaded file list, get files to process
    files_to_process = list(filter(lambda l: (l.replace(path_name + '/', '') in files_of_date_range), downloaded_file_list))
    return files_to_process


dag = DAG('restaurant_pipeline', default_args=default_args)


begin_task = BashOperator(
    task_id='begin_task',
    bash_command="echo 'start'",
    dag=dag)


download_files = SubDagOperator(
    task_id='download_files',
    subdag=sub_download_dag('restaurant_pipeline', 'download_files', d.today(), generate_download_datelist()),
    dag=dag
)


end_task = DummyOperator(
    task_id='end_task',
    dag=dag,
)


initialize_transaction_table_dim_business = PythonOperator(
    task_id='initialize_transaction_table_dim_business',
    python_callable=initialize_transaction_table_dim_business,
    op_kwargs={'db': db},
    dag=dag)


prepare_and_load_transaction_table_dim_business = PythonOperator(
    task_id='prepare_and_load_transaction_table_dim_business',
    python_callable=prepare_and_load_transaction_table_dim_business,
    op_kwargs={'engine': engine},
    dag=dag)


update_transaction_table_dim_business_boro = PythonOperator(
    task_id='update_transaction_table_dim_business_boro',
    python_callable=update_transaction_table_dim_business_boro,
    op_kwargs={'db': db},
    dag=dag)


update_dim_business = PythonOperator(
    task_id='update_dim_business',
    python_callable=update_dim_business,
    op_kwargs={'db': db},
    dag=dag)


remove_updated_dim_business = PythonOperator(
    task_id='remove_updated_dim_business',
    python_callable=remove_updated_dim_business,
    op_kwargs={'db': db},
    dag=dag)


insert_updated_dim_business = PythonOperator(
    task_id='insert_updated_dim_business',
    python_callable=insert_updated_dim_business,
    op_kwargs={'db': db},
    dag=dag)



initialize_transaction_table_dim_grade = PythonOperator(
    task_id='initialize_transaction_table_dim_grade',
    python_callable=initialize_transaction_table_dim_grade,
    op_kwargs={'db': db},
    dag=dag)


prepare_and_load_transaction_table_dim_grade = PythonOperator(
    task_id='prepare_and_load_transaction_table_dim_grade',
    python_callable=prepare_and_load_transaction_table_dim_grade,
    op_kwargs={'engine': engine},
    dag=dag)


remove_updated_dim_grade = PythonOperator(
    task_id='remove_updated_dim_grade',
    python_callable=remove_updated_dim_grade,
    op_kwargs={'db': db},
    dag=dag)


insert_updated_dim_grade = PythonOperator(
    task_id='insert_updated_dim_grade',
    python_callable=insert_updated_dim_grade,
    op_kwargs={'db': db},
    dag=dag)


#########################################################################
########################## DAG flow #####################################

download_files.set_upstream(begin_task)
download_files.set_downstream(end_task)
end_task.set_downstream(initialize_transaction_table_dim_business)

initialize_transaction_table_dim_business.set_downstream(prepare_and_load_transaction_table_dim_business)
prepare_and_load_transaction_table_dim_business.set_downstream(update_transaction_table_dim_business_boro)
update_transaction_table_dim_business_boro.set_downstream(update_dim_business)
update_dim_business.set_downstream(remove_updated_dim_business)
remove_updated_dim_business.set_downstream(insert_updated_dim_business)

initialize_transaction_table_dim_grade.set_upstream(remove_updated_dim_business)
initialize_transaction_table_dim_grade.set_downstream(prepare_and_load_transaction_table_dim_grade)
prepare_and_load_transaction_table_dim_business.set_downstream(remove_updated_dim_grade)
remove_updated_dim_grade.set_downstream(insert_updated_dim_grade)

########################## DAG flow #####################################
#########################################################################


