# Проектная работа 7-го спринта

## Описание
Спроектировано HDFS-хранилище по методу Data Lake. 
Витрины данных были спроектированы и рассчитаны с использованием PySpark.
Расчет витрин был автоматизирован с помощью SparkSubmitOperator в DAG Airflow.

## Структура папок
В `src` находятся 3 папки:
    `/src/pic` - скриншот
    `/src/dags` - с дагом Airflow
    `/src/scripts` - с файлами для загрузки данных и формирования витрин

### Шаг 1. Обновить структуру Data Lake

1.1 Stagging layer:
    `/user/master/data/geo/events`
    `https://code.s3.yandex.net/data-analyst/data_engeneer/geo.csv`

1.2 ODS layer:
    `/user/cgbeavers/data/analytics/proj7_repartition`    - события разложенные по партициям из STG
    `/user/cgbeavers/data/analytics/proj7/cities/geo.csv` - данные по названиям и координатам городов 

1.3 Предобработка и загрузка в ODS:
    `/src/scripts/parquet_event_pre_process.py`

1.4 Analytics sandbox layer:
    `/user/cgbeavers/data/prod/user_mart`         - витрина в разрезе пользователей
    `/user/cgbeavers/data/prod/zones_mart`        - витрина в разрезе зон
    `/user/cgbeavers/data/prod/geo_activity_mart` - витрина для рекомендации друзей

1.5 Формат данных:

STG/events    - `parquet` 
STG/geo.csv   - `csv` 
ODS           - `parquet` 
Sandbox       - `parquet` 

## Шаг 2. Создать витрину в разрезе пользователей

- Файл витрины: `src/scripts/mart_users.py`
```
+-------+----------+---------+------------+--------------------+-------------------+
|user_id|  act_city|home_city|travel_count|        travel_array|         local_time|
+-------+----------+---------+------------+--------------------+-------------------+
|     26| Newcastle|     null|           1|         [Newcastle]|2022-05-13 11:12:00|
|   1806|    Mackay|     null|           5|[Mackay, Launcest...|2021-05-21 05:11:00|
|   3091|  Brisbane|     null|           1|          [Brisbane]|2022-05-01 20:05:00|
|   7747|   Bunbury|     null|           1|           [Bunbury]|2021-04-28 15:55:00|
|   8440|   Bendigo|     null|           1|           [Bendigo]|2021-05-03 04:10:00|
|  10959|    Darwin|   Darwin|           1|            [Darwin]|2022-05-07 17:46:00|
|  11945|   Bendigo|     null|           1|           [Bendigo]|2021-04-25 12:07:00|
|  12044|  Adelaide|     null|           1|          [Adelaide]|2022-05-19 14:00:00|
|  13248|  Brisbane|     null|           1|          [Brisbane]|2021-04-27 11:16:00|
|  13518|     Perth|     null|           1|             [Perth]|2021-05-08 17:54:00|
|  14117|Launceston|     null|           1|        [Launceston]|2022-05-04 03:39:00|
|  15437| Toowoomba|     null|           1|         [Toowoomba]|2021-04-25 09:13:00|
|  15846|Launceston|     null|           1|        [Launceston]|2022-05-08 10:06:00|
|  20532|  Brisbane|     null|           1|          [Brisbane]|2021-04-25 16:44:00|
|  21209| Melbourne|     null|           3|[Melbourne, Macka...|2021-05-02 20:40:00|
|  21223|Wollongong|     null|           1|        [Wollongong]|2022-05-03 04:21:00|
|  21342|   Bunbury|     null|           1|           [Bunbury]|2022-05-16 09:38:00|
|  23766|Wollongong|     null|           1|        [Wollongong]|2021-05-16 20:13:00|
|  25207| Newcastle|     null|           5|[Newcastle, Towns...|2021-05-03 10:32:00|
|  26543| Melbourne|     null|           1|         [Melbourne]|2021-04-29 02:00:00|
+-------+----------+---------+------------+--------------------+-------------------+
```
## Шаг 3. Создать витрину в разрезе зон

- Файл витрины: `src/scripts/mart_zones.py`
```
+----------+----------+-------+------------+-------------+-----------------+---------+-------------+--------------+------------------+----------+
|month     |week      |zone_id|week_message|week_reaction|week_subscription|week_user|month_message|month_reaction|month_subscription|month_user|
+----------+----------+-------+------------+-------------+-----------------+---------+-------------+--------------+------------------+----------+
|2022-04-01|2022-04-18|22     |93          |17           |0                |106      |686          |126           |0                 |378       |
|2022-04-01|2022-04-18|4      |190         |26           |0                |15       |1167         |140           |0                 |63        |
|2022-04-01|2022-04-18|5      |72          |19           |0                |104      |490          |150           |0                 |390       |
|2022-04-01|2022-04-25|19     |2339        |754          |0                |225      |2409         |725           |0                 |288       |
|2022-04-01|2022-04-25|2      |1864        |751          |0                |291      |2001         |742           |0                 |376       |
|2022-04-01|2022-04-25|21     |986         |134          |0                |215      |1076         |133           |0                 |262       |
|2022-04-01|2022-04-25|9      |1040        |537          |0                |170      |1112         |522           |0                 |215       |
|2022-05-01|2022-04-25|3      |1977        |631          |0                |290      |3667         |2939          |0                 |328       |
|2022-05-01|2022-04-25|4      |1117        |138          |0                |421      |1882         |657           |0                 |463       |
|2022-05-01|2022-05-02|19     |1414        |813          |0                |110      |3340         |3081          |0                 |242       |
|2022-05-01|2022-05-02|22     |460         |141          |0                |101      |1405         |566           |0                 |239       |
|2022-05-01|2022-05-09|16     |145         |169          |0                |112      |463          |566           |0                 |328       |
|2022-05-01|2022-05-09|17     |474         |156          |0                |20       |1336         |533           |0                 |73        |
|2022-05-01|2022-05-09|3      |1124        |929          |0                |15       |3667         |2939          |0                 |54        |
|2022-05-01|2022-05-16|11     |33          |202          |0                |41       |156          |606           |0                 |239       |
|2022-05-01|2022-05-16|12     |189         |241          |0                |41       |892          |742           |0                 |239       |
|2022-05-01|2022-05-16|12     |189         |241          |0                |32       |892          |742           |0                 |199       |
|2022-05-01|2022-05-16|16     |83          |165          |0                |10       |463          |566           |0                 |82        |
|2022-05-01|2022-05-23|18     |5           |60           |0                |8        |320          |648           |0                 |242       |
|2022-05-01|2022-05-23|20     |32          |73           |0                |2        |1649         |721           |0                 |82        |
+----------+----------+-------+------------+-------------+-----------------+---------+-------------+--------------+------------------+----------+
```
## Шаг 4. Создать витрину для рекомендации друзей

- Джоба для теста например на Jupyter: `src/scripts/mart_geo_activity.py`
```
+---------+----------+--------------------+----------+-------------------+
|user_left|user_right|      processed_dttm|   zone_id|         local_time|
+---------+----------+--------------------+----------+-------------------+
|   136806|     74981|2023-09-25 22:52:...| Melbourne|2022-04-30 19:20:35|
|    34709|     69338|2023-09-25 22:52:...|Cranbourne|               null|
|    74979|     74981|2023-09-25 22:52:...| Melbourne|               null|
|     3838|    130799|2023-09-25 22:52:...|   Bendigo|               null|
|    72278|     74981|2023-09-25 22:52:...| Melbourne|               null|
|    64004|     71912|2023-09-25 22:52:...|Launceston|               null|
|   136780|     46589|2023-09-25 22:52:...|Cranbourne|               null|
|    50759|      1229|2023-09-25 22:52:...|   Ipswich|               null|
|   159731|    127102|2023-09-25 22:52:...|  Brisbane|               null|
|    61311|     99253|2023-09-25 22:52:...|Launceston|               null|
|     2065|    129212|2023-09-25 22:52:...|    Mackay|               null|
+---------+----------+--------------------+----------+-------------------+
```
## Шаг 5. Автоматизировать обновление витрин

  `/src/dags/dag.py`

### commands

Local terminal:
`ssh -i ssh_private_key.file yc-user@ip_address`

Copy file to Hadoop:
`! hdfs dfs -copyFromLocal /lessons/geo.csv /user/{username}/analytics/proj7/cities/`
