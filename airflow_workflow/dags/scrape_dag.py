# coding: utf-8
from datetime import datetime
from os import path

from airflow import DAG
from airflow.providers.discord.operators.discord_webhook import DiscordWebhookOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.sql import SQLCheckOperator
from airflow.operators.python_operator import PythonOperator
import xe_rentals_scrape.py



default_args = {"start_date": datetime(2024, 1, 27), 'retries':1, 'owner': 'airflow'}

"""
Create a DAG that runs the above scripts with the following properties:

DAG name: compute_orders_not_in_stock
Schedule: Manually
Dependencies: Tasks should run sequentially (a -> b -> c)
Trigger rules: Task c should run if task b fails (i.e. table has more than 5 records)
"""

searchpath = path.abspath(path.join(path.dirname(__file__), "../../../plugins"))

with DAG(
    "webscrape_xe_gr",
    schedule_interval=None,
    template_searchpath=searchpath,
    default_args=default_args,
) as dag:

    webscrape= PythonOperator(
        task_id='scrape_xe_gr',
        python_callable="xe_rentals.py",
        dag=dag
        )




















    # compute_out_of_order_stock = PostgresOperator(
    #     task_id="compute_out_of_order_stocks",
    #     sql="update_analytics_table.sql",
    #     postgres_conn_id="bike_db",
    # )

    # less_than_5_validation = SQLCheckOperator(
    #     task_id="less_than_5_validation", sql="validation.sql", conn_id="bike_db"
    # )

    # warning_notification = DiscordWebhookOperator(
    #     task_id="warning_notification",
    #     http_conn_id="http_conn_id",
    #     message="Caution! More than 5 orders have products out of stock!",
    #     trigger_rule="one_failed",
    # )

    # compute_out_of_order_stocks >> less_than_5_validation >> warning_notification
