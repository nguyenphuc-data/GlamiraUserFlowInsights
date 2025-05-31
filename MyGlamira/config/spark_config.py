from typing import Optional, List, Dict
from pyspark.sql import SparkSession
import os
from config.database_config import get_database_config

def create_spark_session(
    app_name : str,
    master_url : str = "local[*]",
    executor_memory : Optional[str] = "4g",
    executor_cores : Optional[str] = 2,
    driver_memory : Optional[str] = "2g",
    num_executors : Optional[int] = 3,
    jars : Optional[List[str]] = None,
    spark_conf : Optional[Dict[str,str]] = None,
    log_level : str = "INFO"
) -> SparkSession:
    builder = SparkSession.builder \
        .appName(app_name) \
        .master(master_url)

    if executor_memory:
        builder.config("spark.executor.memory",executor_memory)
    if executor_cores:
        builder.config("spark.executor.cores",executor_cores)
    if driver_memory:
        builder.config("spark.driver.memory",driver_memory)
    if num_executors:
        builder.config("spark.executor.instances",num_executors)
    if jars:
        jars_path = ".".join([os.path.abspath(jar) for jar in jars])
        builder.config("spark.jars",jars_path)

    # {"spark.sql.shuffle.partitions" : "10" }
    if spark_conf:
        for key, value in spark_conf.items():
            builder.config(key,value)

    spark = builder.getOrCreate() # Must start system -> having log

    spark.sparkContext.setLogLevel(log_level)

    return spark

# def connect_to_mysql(spark : SparkSession, config : Dict[str,str], table_name : str):
#     df = spark.read \
#         .format("jdbc") \
#         .option("url", "jdbc:mysql://172.17.0.2:3306/glamira_data") \
#         .option("dbtable", table_name) \
#         .option("user", config["user"]) \
#         .option("password", config["password"]) \
#         .option("driver", "com.mysql.cj.jdbc.Driver") \
#         .load()
#     return df
#
# jar_path = "/home/nguyenphuc/Documents/GlamiraUserFlowInsightsProject/GlamiraUserFlowInsights/MyGlamira/lib/mysql-connector-j-9.2.0.jar"
# spark = create_spark_session(
#     app_name = "phuc",
#     master_url = "local[*]",
#     excutor_memory = "4g",
#     jars = [jar_path],
#     log_level = "INFO"
# )
#
# db_config = get_database_config()
# mysql_table = "currency_totals"
#
# df = connect_to_mysql(spark,db_config,mysql_table)
# df.show()
