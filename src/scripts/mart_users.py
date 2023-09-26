import sys
import os
import pyspark.sql.functions as F
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

#часть для jupiter
#import pyspark
#import findspark
#findspark.init()
#findspark.find()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession, DataFrame
from pyspark.sql.window import Window 
from pyspark.sql.types import DateType
from datetime import datetime, timedelta
from math import radians, cos, sin, asin, sqrt

# расчёты distance
def get_distance(lon_a, lat_a, lon_b, lat_b):
    lon_a, lat_a, lon_b, lat_b = map(radians, [lon_a,  lat_a, lon_b, lat_b])
    dist_longit = lon_b - lon_a
    dist_latit = lat_b - lat_a
    area = sin(dist_latit/2)**2 + cos(lat_a) * cos(lat_b) * sin(dist_longit/2)**2
    central_angle = 2 * asin(sqrt(area))
    radius = 6371
    distance = central_angle * radius

    return abs(round(distance, 2))

udf_get_distance = F.udf(get_distance)

def main() -> None:
# часть для airflow
    events_path = sys.argv[1]
    cities_data_path = sys.argv[2]
    output_path = sys.argv[3]

# пути
   # events_path = "/user/cgbeavers/data/analytics/proj7_repartition/"
# events_path = "/user/master/data/geo/events/"
   # cities_data_path = "/user/cgbeavers/data/analytics/proj7/cities/geo.csv"
    #output_path = "/user/cgbeavers/data/prod/user_mart/"
    
# сессия
#    spark = SparkSession.builder \
#                   .master("local") \
#                    .appName("project_7") \
 #                   .getOrCreate()
    conf = SparkConf().setAppName(f"mart_users")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc) 
    
# Загрузка сообщений
    df_message = (sql
      .read.parquet(f'{events_path}')
      .where("event.message_from IS NOT NULL")
      .select('event.message_from'
            ,F.date_trunc("minute",F.coalesce(F.col('event.datetime'),F.col('event.message_ts'))).alias("date")
            ,'lat', 'lon').distinct()
      .selectExpr('message_from as user_id', "date",'lat', 'lon')
      .persist())
#    print('df_message')
#    df_message.show()
    
# Загрузка csv с городами
    df_csv = sql.read.option("header", True).option("delimiter", ";").csv(f'{cities_data_path}')
    df_csv = df_csv.withColumn("lat",F.regexp_replace("lat", ",", ".")).withColumn("lng",F.regexp_replace("lng",",","."))
    df_citygeodata = (df_csv.select(F.col("id").alias("city_id"),(F.col("city"))
                                   .alias("city_name"),(F.col("lat")).cast('double').alias("city_lat"),(F.col("lng"))
                                   .cast('double').alias("city_lon")).persist())
#    print('df_citygeodata')
#    df_citygeodata.show()
    
# События умноженные на список городов
    df_message_and_citygeodata = (df_message.crossJoin(df_citygeodata.hint("broadcast")).persist())    
#    print('df_message_and_citygeodata')
#    df_message_and_citygeodata.show()

    df_message_and_distance = (df_message_and_citygeodata.select(
        "user_id","date","city_name"
        ,udf_get_distance(
        F.col("lon"), F.col("lat"), F.col("city_lon"), F.col("city_lat")
        ).cast('double').alias("distance")).withColumn("row",F.row_number().over(
        Window.partitionBy("user_id","date").orderBy(F.col("distance"))
        )).filter(F.col("row") ==1)
        .drop("row", "distance")
        .persist())
#    print('df_message_and_distance')
#    df_message_and_distance.show()
    
# расчет актуального города
    df_act_city = (df_message_and_distance.withColumn("row",F.row_number()
        .over(Window.partitionBy("user_id").orderBy(F.col("date").desc())))
        .filter(F.col("row") == 1)
        .drop("row", "distance")
        .withColumnRenamed("city_name", "act_city")
        .persist())
#    print('df_act_city')
#    df_act_city.show()

# Temp DF
    df_change = (df_message_and_distance.withColumn('max_date',F.max('date')
        .over(Window().partitionBy('user_id')))
        .withColumn('city_name_lag_1_desc',F.lag('city_name',-1,'none')
        .over(Window().partitionBy('user_id').orderBy(F.col('date').desc())))
        .filter(F.col('city_name') != F.col('city_name_lag_1_desc'))
        .persist())
#    print('df_change')
#    df_change.show()

# расчет домашнего города
    df_home_city = (df_change.withColumn('date_lag',F.coalesce(F.lag('date')
        .over(Window().partitionBy('user_id').orderBy(F.col('date').desc())),F.col('max_date')))
        .withColumn('date_diff',F.datediff(F.col('date_lag'),F.col('date')))
        .filter(F.col('date_diff') > 27)
        .withColumn('row',F.row_number()
        .over(Window.partitionBy("user_id").orderBy(F.col("date").desc())))
        .filter(F.col('row') == 1)
        .drop('date','city_name_lag_1_desc','date_lag','row','date_diff','max_date')
        .persist())
#    print('df_home_city')
#    df_home_city.show()
    
# количество посещённых городов
    df_travel_count = (df_change.groupBy("user_id").count().withColumnRenamed("count", "travel_count"))
    
# список городов в порядке посещения
    df_travel_array = (df_change
        .groupBy("user_id")
        .agg(F.collect_list('city_name').alias('travel_array'))
        .persist())
#    print('df_travel_array')
#    df_travel_array.show()

# местное время
    df_local_time = (df_act_city.withColumn('city_true', (F.when((F.col('act_city') != 'Gold Coast') & (F.col('act_city') != 'Cranbourne')   
                            & (F.col('act_city') != 'Newcastle') 
                            & (F.col('act_city') != 'Wollongong') & (F.col('act_city') != 'Geelong') & (F.col('act_city') != 'Townsville') 
                            & (F.col('act_city') != 'Ipswich') & (F.col('act_city') != 'Cairns') & (F.col('act_city') != 'Toowoomba') 
                            & (F.col('act_city') != 'Ballarat') & (F.col('act_city') != 'Bendigo') & (F.col('act_city') != 'Launceston') 
                            & (F.col('act_city') != 'Mackay') & (F.col('act_city') != 'Rockhampton') & (F.col('act_city') != 'Maitland') 
                            & (F.col('act_city') != 'Bunbury'), F.col('act_city')).otherwise('Brisbane')))\
                                .withColumn('TIME', (F.col('date').cast("Timestamp")))\
                                .withColumn('timezone', F.concat(F.lit('Australia'), F.lit('/'),  F.col('city_true')))\
                                .withColumn('local_time', F.from_utc_timestamp(F.col('TIME'), F.col('timezone')))
                     .drop("timezone", "date", "act_city")
                     .persist())
#    df_local_time.show()

# витрина в разрезе пользователей. объединения всех метрик
    df_user_analitics_mart = (df_act_city.select("user_id", "act_city")
        .join(df_home_city, 'user_id', how='left')
        .join(df_travel_count, 'user_id', how='left')
        .join(df_travel_array, 'user_id', how='left')
        .join(df_local_time, 'user_id', how='left')
    .selectExpr("user_id", "act_city", 'city_name as home_city', "travel_count", 'travel_array', 'local_time'))
#    print('df_user_analitics_mart')
    df_user_analitics_mart.show()
    
# Сохранение витрины на hdfs 
    df_user_analitics_mart.write.mode("overwrite").parquet(f"{output_path}")


if __name__ == "__main__":
        main()