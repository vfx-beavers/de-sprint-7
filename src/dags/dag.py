import os
import airflow
import sys
import logging
import pyspark.sql.functions as F 

import findspark
findspark.init()
findspark.find()

from pyspark.sql import SparkSession
from datetime import timedelta, date, datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pyspark.sql.window import Window 
from pyspark.sql.types import *

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
    'owner': 'airflow',
    'start_date':datetime(2022, 1, 1),
}

dag_spark = DAG(
    dag_id = "datalake",
    default_args=default_args,
    schedule_interval=None,
)

mart_users = SparkSubmitOperator(
    task_id='mart_users',
    dag=dag_spark,
    application ='/lessons/scripts/mart_users.py' ,
    conn_id= 'yarn_spark',
    application_args = [ 
        "/user/cgbeavers/data/analytics/proj7_repartition/", 
        "/user/cgbeavers/data/analytics/proj7/cities/geo.csv", 
        "/user/cgbeavers/data/prod/user_mart/"
        ],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores = 2,
    executor_memory = '4g'
)

mart_zones = SparkSubmitOperator(
    task_id='mart_zones',
    dag=dag_spark,
    application ='/lessons/scripts/mart_zones.py' ,
    conn_id= 'yarn_spark',
    application_args = [
        "/user/cgbeavers/data/analytics/proj7_repartition/", 
        "/user/cgbeavers/data/analytics/proj7/cities/geo.csv",
        "/user/cgbeavers/data/prod/zones_mart/"],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores = 2,
    executor_memory = '4g'
)

mart_geo_activity = SparkSubmitOperator(
    task_id='mart_geo_activity',
    dag=dag_spark,
    application ='/lessons/scripts/mart_geo_activity.py' ,
    conn_id= 'yarn_spark',
    application_args = [
        "/user/cgbeavers/data/analytics/proj7_repartition/", 
        "/user/cgbeavers/data/analytics/proj7/cities/geo.csv", 
        "/user/cgbeavers/data/prod/geo_activity_mart/"],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores = 2,
    executor_memory = '4g'
)

mart_users >> mart_zones >> mart_geo_activity

