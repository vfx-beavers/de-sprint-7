import sys
import os
import lib
from pyspark.sql.functions import col, row_number, current_date, date_format
import pyspark.sql.functions as F
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

global pi, coef_deg_rad 
pi = 3.14159265359
coef_deg_rad = pi/180

#для jupiter
#import pyspark
#import findspark
#findspark.init()
#findspark.find()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession, DataFrame
from pyspark.sql.window import Window 
from pyspark.sql.types import *
from datetime import datetime


def main() -> None:
# пути 
    events_path = sys.argv[1]
    cities_data_path = sys.argv[2]
    output_path = sys.argv[3]
    
    #events_path = "/user/cgbeavers/data/analytics/proj7_repartition/"
    # events_path = "/user/master/data/geo/events/"
    #cities_data_path = "/user/cgbeavers/data/analytics/proj7/cities/geo.csv"
    #output_path = "/user/cgbeavers/data/prod/geo_activity_mart/"

# сессия
#   spark = SparkSession.builder \
#                    .master("yarn") \
#                    .config("spark.driver.memory", "4g") \
#                    .config("spark.driver.cores", 2) \
#                    .appName("p7") \
 #                   .getOrCreate()
    conf = SparkConf().setAppName("mart_geo_activity")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    cities_df = cities(cities_data_path, sql)
    events_filtered_df = events_filtered(events_path, sql)
    city_events_data_raw_df = city_events_data_raw(cities_df, events_filtered_df)
    city_events_data_df = city_events_data(city_events_data_raw_df)
    subscribers_with_distance_df = subscribers_with_distance(city_events_data_df)
    non_tinder_users_df = non_tinder_users(city_events_data_df)
    local_time_df = local_time_f(events_path, sql)
    geo_activity_mart_df = geo_activity_mart(city_events_data_df, local_time_df)
    write = writer(geo_activity_mart_df, output_path)


# df городов, градусы в радианы, сброс колонок
def cities(cities_data_path: str, sql) -> DataFrame:
    cities_df = (sql.read.option("header", True)
            .option("delimiter", ";")
            .option("inferSchema", True)
            .csv(f'{cities_data_path}')
            .withColumn('lat_n', F.regexp_replace('lat', ',' , '.').cast('double'))
            .withColumn('lng_n', F.regexp_replace('lng', ',' , '.').cast('double'))
            .withColumn('city_lat',F.col('lat_n')*F.lit(coef_deg_rad))
            .withColumn('city_long',F.col('lng_n')*F.lit(coef_deg_rad))
            .select('id', 'city', 'city_lat', 'city_long'))
    
#    print('cities_df')
#    cities_df.show(5)
    return cities_df


# df фильтрованных событий, градусы в радианы, сброс колонок
def events_filtered(events_path: str, sql) -> DataFrame:
    events_filtered_df = (sql
                  .read.parquet(f'{events_path}')
#                  .where("event_type ='subscription'")
#                  .where("event.message_from IS NOT NULL")
                  .withColumn('lat',F.col('lat')*F.lit(coef_deg_rad))
                  .withColumn('lon',F.col('lon')*F.lit(coef_deg_rad))
                  .where('lat IS NOT NULL and lon IS NOT NULL')
                  .select('event', 'event_type', 'date', 'lat', 'lon'))
    
#    print('events_filtered_df')
#    events_filtered_df.show(5)
    return events_filtered_df


# кросс событий на города
def city_events_data_raw(cities_df: DataFrame, events_filtered_df: DataFrame) -> DataFrame:
    city_events_data_raw_df = events_filtered_df.crossJoin(cities_df)
    
#    city_events_data_raw_df.show(5)
    return city_events_data_raw_df


# события города и дистанция
def city_events_data(city_events_data_raw_df: DataFrame) -> DataFrame:
    events = city_events_data_raw_df
    
    city_events_data_df = (lib.distance(data=events, first_lat='lat', second_lat='city_lat', first_lon='lon', second_lon='city_long') \
        .select('event', 'event_type', 'id', 'city', 'date', 'lat', 'lon', 'distanse') \
        .withColumn("row_number", row_number().over(Window.partitionBy("event").orderBy("distanse"))) \
        .where("row_number=1") #.drop('row_number')
       ).persist()
    
#    print('city_events_data_df')
#    city_events_data_df.show(5)
    return city_events_data_df
    
# проверка подписок и расстояния между пользователями
def subscribers_with_distance(city_events_data_df: DataFrame):
    user_sub = city_events_data_df \
        .where("event_type ='subscription'") \
        .select(F.col('event.user').alias('user_left'), 'event.subscription_channel', F.col('lat').alias('user_lat'),
                F.col('lon').alias('user_lon'), 'id', 'city')
    user_sub2 = user_sub.select(col('user_left').alias('user_right'), 'subscription_channel',
                                col('user_lat').alias('contact_lat'), col('user_lon').alias('contact_lon'),
                                col('id').alias('r_u_id'), col('city').alias('c_city'))

    all_subscribers = user_sub.join(user_sub2, 'subscription_channel', 'inner').where('user_left < user_right').distinct()
    subscribers_with_distance_df = (lib.distance(data=all_subscribers, first_lat='user_lat', second_lat='contact_lat',
                                              first_lon='user_lon', second_lon='contact_lon').where('distanse is not null').where(
        'distanse < 1.0').select('user_left', 'user_right', 'id', 'city')).persist()
    
#    print('subscribers_with_distance_df')
#    subscribers_with_distance_df.show(5)
    return subscribers_with_distance_df
    
# проверка пользователей которые не общались. кросс левых на правых
def non_tinder_users(city_events_data_df: DataFrame):
    
    out_user_contacts = city_events_data_df \
        .select(col('event.message_from').alias('user_left'), col('event.message_to').alias('user_right')) \
        .where("event_type ='message'")
    
    receive_user_contacts = city_events_data_df \
        .select(col('event.message_to').alias('user_left'), col('event.message_from').alias('user_right')) \
        .where("event_type ='message'")
    non_tinder_users_df = (out_user_contacts.union(receive_user_contacts)).distinct().persist()
    
#    print('non_tinder_users_df')
#    non_tinder_users_df.show(5)
    return non_tinder_users_df


# Локальное время 
def local_time_f(events_path: str, sql) -> DataFrame:
    times = (
        sql.read.parquet(f'{events_path}').where('event_type = "message"')
        .selectExpr("event.message_from as user_id", "event.datetime", "event.message_id")
        .where("datetime IS NOT NULL"))
    
    window_t = Window().partitionBy('user_id').orderBy(F.col('datetime').desc())

    local_time_df = (times
            .withColumn("row_number", F.row_number().over(window_t))
            .filter(F.col('row_number')==1)
            .withColumn("TIME",F.col("datetime").cast("Timestamp"))
            .selectExpr("user_id as user_left", "Time")
            ).persist()
    
#    print('local_time_df')
#    local_time_df.show()
    return local_time_df

# сборка витрины рекомендации друзей
def geo_activity_mart(city_events_data_df: DataFrame, local_time_df: DataFrame):
    subscribers = subscribers_with_distance(city_events_data_df)
    no_message_users = non_tinder_users(city_events_data_df)
    
    geo_activity_mart_df = (subscribers
        .join(no_message_users, ['user_left', 'user_right'], 'leftanti') \
        .withColumn("processed_dttm", current_date()) \
        .join(local_time_df, 'user_left', how='left') \
        .withColumn('local_time', date_format(col('Time'), 'HH:mm:ss')) \
        .selectExpr('user_left', 'user_right', 'processed_dttm', 'id as zone_id', "local_time"))
    
#    print('geo_activity_mart_df')
#    geo_activity_mart_df.show()
    return geo_activity_mart_df


# запись витрины
def writer(geo_activity_mart_df, output_path):
    return geo_activity_mart_df \
        .write \
        .mode('overwrite') \
        .parquet(f'{output_path}')


if __name__ == "__main__":
        main()