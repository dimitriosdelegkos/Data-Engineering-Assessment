# Description: This script is an automated pipeline that extracts data from 2 csv files, transforms it and loads it into a postgres database.
# Note: I used pandas to do the ETL process (instead of Spark)

from datetime import datetime, timedelta
from sqlalchemy import create_engine

import airflow
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

import pandas as pd

default_args = {'owner': 'dimitris', 'depends_on_past': False,
                'start_date': airflow.utils.dates.days_ago(1)}


def extract():
    user_browsing_df = pd.read_csv(
        '/usr/local/airflow/dags/Data Engineering Assessment data/dataset1.csv')
    user_transactions_df = pd.read_csv(
        '/usr/local/airflow/dags/Data Engineering Assessment data/dataset2.csv')
    return user_browsing_df, user_transactions_df


def transform():
    user_browsing_df, user_transactions_df = extract()
    # Rename the timestamp column to avoid confusion
    user_transactions_df.rename(
        columns={'timestamp': 'transaction_timestamp'}, inplace=True)
    # Drop the user column as it is not needed (i have it on the other table)
    user_transactions_df.drop(columns=['user'], inplace=True)
    # Join the 2 tables on the session_id column
    user_joined_df = pd.merge(
        user_browsing_df, user_transactions_df, on='session_id', how='inner')
    # Convert the timestamp and transaction_timestamp columns to datetime objects
    user_joined_df['timestamp'] = pd.to_datetime(
        user_joined_df['timestamp'], unit='s')
    user_joined_df['transaction_timestamp'] = pd.to_datetime(
        user_joined_df['transaction_timestamp'], unit='s')
    # create a new column with the difference (in days) between the transaction_timestamp and timestamp columns
    user_joined_df['dates_diff'] = (
        user_joined_df['transaction_timestamp'] - user_joined_df['timestamp']).dt.days
    # filter the observations with dates_diff = 0 or 1
    user_without_outliers_df = user_joined_df[user_joined_df['dates_diff'].isin([
                                                                                0, 1])]
    # Since transaction A and B are equivalent, I will convert them into the same.
    user_without_outliers_df.replace({'transaction': {'B': 'A'}}, inplace=True)
    # Finally, i will drop the dates_diff column as it is not needed anymore
    user_without_outliers_df.drop(columns=['dates_diff'], inplace=True)
    # Create the 3 tables
    # Select only the unique users
    users = user_without_outliers_df.drop_duplicates(subset=['user'])['user']
    users.rename('userid', inplace=True)
    # Select only the unique sessions
    sessions = user_without_outliers_df.drop_duplicates(subset=['session_id', 'user'])[
        ['session_id', 'transaction', 'transaction_timestamp', 'user']]
    sessions.rename(columns={'user': 'userid'}, inplace=True)
    # Create an intermediate table
    session_details = user_without_outliers_df[[
        'session_id', 'page', 'timestamp']]
    session_details.rename(columns={'timestamp': 'stimestamp'}, inplace=True)
    return users, sessions, session_details


def load():
    users, sessions, session_details = transform()
    # Establish a connection with the database
    postgres_hook = PostgresHook(
        postgres_conn_id='postgres_default', schema='airflow')
    pg_conn = postgres_hook.get_conn()
    engine = create_engine(postgres_hook.get_uri())
    # Insert data into the database
    users.to_sql(name='nusers', con=engine,
                 if_exists='replace', index=False)
    sessions.to_sql(name='sessions', con=engine,
                    if_exists='replace', index=False)
    session_details.to_sql(name='session_details', con=engine,
                           if_exists='replace', index=False)
    return "Data loaded successfully"


with DAG('automated_pipeline', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:

    task1 = PythonOperator(task_id='extract',
                           python_callable=extract)

    task2 = PythonOperator(task_id='transform',
                           python_callable=transform)

    task3 = PythonOperator(task_id='load',
                           python_callable=load)

    task1 >> task2 >> task3
