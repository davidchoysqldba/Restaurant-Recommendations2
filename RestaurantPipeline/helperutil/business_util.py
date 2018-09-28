import pandas as pd
import logging

log = logging.getLogger(__name__)

import sys
# import os

def get_dataframe_from_files():
    sys.path.insert(0, '../')
    from restaurant_pipeline import get_process_file_list

    files_to_process = get_process_file_list()
    dataframes = (pd.read_json(f, lines=True, encoding='UTF8') for f in files_to_process)
    big_dataframe = pd.concat(dataframes, ignore_index=True)
    big_dataframe.reset_index()
    return big_dataframe


def get_business_list_in_dataframe():

    # sys.path.insert(0, '../')
    # from restaurant_pipeline import get_process_file_list
    #
    # files_to_process = get_process_file_list()
    # dataframes = (pd.read_json(f, lines=True, encoding='UTF8') for f in files_to_process)
    # big_dataframe = pd.concat(dataframes, ignore_index=True)
    # big_dataframe.reset_index()

    big_dataframe = get_dataframe_from_files()
    big_dataframe = big_dataframe[['camis', 'dba', 'building', 'street', 'zipcode', 'boro', 'record_date']].drop_duplicates(subset=['camis', 'dba', 'building', 'street', 'zipcode', 'boro', 'record_date'])
    big_dataframe.columns = ['business_number', 'business_title', 'address_number', 'address_name', 'zipcode', 'boro', 'record_date']
    big_dataframe = big_dataframe[big_dataframe['record_date'] == big_dataframe.groupby(['business_number'])['record_date'].transform(max)]
    return big_dataframe


def get_grade_list_in_dataframe():
    big_dataframe = get_dataframe_from_files()
    big_dataframe = big_dataframe[['camis', 'grade', 'grade_date']].drop_duplicates(subset=['camis', 'grade', 'grade_date'])
    big_dataframe = big_dataframe[big_dataframe['grade_date'].isna() == False]
    big_dataframe.columns = ['business_number', 'grade', 'grade_date']
    return big_dataframe


def initialize_transaction_table_dim_business(db):
    # db = dbUtil(default_args['db_hostname'], default_args['db_username'], default_args['db_password'], default_args['db_database'])
    result = db.executeQuery('truncate table trans_dim_business')
    # log.info(result)


def prepare_and_load_transaction_table_dim_business(engine):
    df_business = get_business_list_in_dataframe()
    df_business.to_sql('trans_dim_business', con=engine, if_exists='append', index=False)
    # I tried to use SQL to insert into the transaction table, but encountered some error which I will resolve later.


def update_transaction_table_dim_business_boro(db):
    # db = dbUtil(default_args['db_hostname'], default_args['db_username'], default_args['db_password'], default_args['db_database'])
    result = db.executeQuery('update trans_dim_business a join dim_boro b on a.boro = b.boro_name set a.boro_id = b.boro_id')


def update_dim_business(db):
    # db = dbUtil(default_args['db_hostname'], default_args['db_username'], default_args['db_password'], default_args['db_database'])
    result = db.executeQuery("""
    update dim_business a join trans_dim_business b on a.business_number = b.business_number
    set
    a.business_title = b.business_title,
    a.address_number = b.address_number,
    a.address_name = b.address_name,
    a.zipcode = b.zipcode,
    a.boro_id = b.boro_id
    """
    )


def remove_updated_dim_business(db):
    # db = dbUtil(default_args['db_hostname'], default_args['db_username'], default_args['db_password'], default_args['db_database'])
    result = db.executeQuery("""
    delete a
    from trans_dim_business a left join dim_business b on a.business_number = b.business_number
    where b.business_number is not null
    """)


def insert_updated_dim_business(db):
    # db = dbUtil(default_args['db_hostname'], default_args['db_username'], default_args['db_password'], default_args['db_database'])
    result = db.executeQuery("""
    insert into dim_business (business_number, business_title, address_number, address_name, zipcode, boro_id, record_date, create_date, modify_date)
    select business_number, business_title, address_number, address_name, zipcode, boro_id, record_date, now(), now() from trans_dim_business
    """)


def initialize_transaction_table_dim_grade(db):
    # db = dbUtil(default_args['db_hostname'], default_args['db_username'], default_args['db_password'], default_args['db_database'])
    result = db.executeQuery('truncate table trans_dim_grade')
    # log.info(result)


def prepare_and_load_transaction_table_dim_grade(engine):
    df_business = get_grade_list_in_dataframe()
    df_business.to_sql('trans_dim_grade', con=engine, if_exists='append', index=False)
    # I tried to use SQL to insert into the transaction table, but encountered some error which I will resolve later.


def remove_updated_dim_grade(db):
    # db = dbUtil(default_args['db_hostname'], default_args['db_username'], default_args['db_password'], default_args['db_database'])
    result = db.executeQuery("""
    delete a
    from trans_dim_grade a left join dim_grade b on a.business_number = b.business_number and a.grade_date = b.grade_date
    where b.business_number is not null
    """)


def insert_updated_dim_grade(db):
    # db = dbUtil(default_args['db_hostname'], default_args['db_username'], default_args['db_password'], default_args['db_database'])
    result = db.executeQuery("""
    insert into dim_grade (business_number, grade, grade_date, create_date, modify_date)
    select business_number, grade, grade_date, now(), now() from trans_dim_grade
    """)
