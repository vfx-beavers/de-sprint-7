import os
import findspark

findspark.init()
findspark.find()
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

def main():
    
# переменные и пути
    sname = "cgbeavers"  # пользователь
    hdfs_path = "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020" # путь hdfs
    geo_path = "/user/master/data/geo/events/" # исходные данные по событиям
    citygeodata_csv = f"{hdfs_path}/user/{sname}/data/analytics/proj7/cities/geo.csv" # исходные данные по городам
    start_date = '2022-05-21' # от какой даты
    depth = 27 # глубина анализа в днях

    global spark
    spark = SparkSession \
     .builder \
     .master("yarn") \
        .config("spark.driver.cores", "2") \
        .config("spark.driver.memory", "2g") \
        .appName("YOUR_APP_NAME") \
        .getOrCreate()
    
# Функция партиционорования данных и сохранения
    for i in range(int(depth)):
        i_date = ((datetime.strptime(start_date, '%Y-%m-%d') - timedelta(days=i)).strftime('%Y-%m-%d'))
        i_input_source_path = hdfs_path + geo_path + "date=" + i_date
        i_output_path = hdfs_path + "/user/" + sname + "/data/analytics/proj7_repartition/date=" + i_date
        
# Чтение нужного диапазона
    events = (
            spark.read
            .option('basePath', f'{i_input_source_path}')
            .parquet(f"{i_input_source_path}")
        )
#    print(f"input: {i_input_source_path}")
#    print(f"output: {i_output_path}")
#    events.show(20)
    
# Сохранение parquet по партиции event_type в соответствующие папки
    events.write.mode('ignore').partitionBy('event_type').parquet(f'{hdfs_path}/user/{sname}/data/analytics/proj7_repartition/date={i_date}')

    parquet_event(start_date=start_date, depth=depth, sname=sname, hdfs_path=hdfs_path, geo_path=geo_path)      
        
if __name__ == '__main__':
    main()