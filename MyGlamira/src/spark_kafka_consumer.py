from config.spark_config import create_spark_session, get_database_config
from pyspark.sql.functions import from_json, col, explode, regexp_replace, from_unixtime, to_timestamp
from pyspark.sql.types import StructType, StringType, LongType, ArrayType, IntegerType, DoubleType
from pyspark import SparkConf
import logging

# Thiết lập logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Tạo SparkSession
conf = SparkConf().setAppName("KafkaETL").set("spark.executor.memory", "4g")
conf.set("spark.ui.showConsoleProgress", "false")
conf.set("spark.ui.port", "4050")
conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
spark = create_spark_session(
    app_name="KafkaETL",
    executor_memory="4g",
    spark_conf={
        "spark.ui.showConsoleProgress": "false",
        "spark.ui.port": "4050",
        "spark.sql.streaming.statefulOperator.checkCorrectness.enabled": "false"
    },
    jars=["/home/nguyenphuc/Documents/GlamiraUserFlowInsightsProject/GlamiraUserFlowInsights/MyGlamira/lib/mysql-connector-j-9.2.0.jar"]
)
spark.sparkContext.setLogLevel("ERROR")

# Định nghĩa schema cho Kafka
schema = StructType() \
    .add("_id", StringType()) \
    .add("time_stamp", LongType()) \
    .add("current_url", StringType()) \
    .add("referrer_url", StringType()) \
    .add("collection", StringType()) \
    .add("cart_products", ArrayType(
        StructType()
        .add("product_id", IntegerType())
        .add("amount", IntegerType())
        .add("price", StringType())
        .add("currency", StringType())
        .add("option", ArrayType(
            StructType()
            .add("option_label", StringType())
            .add("option_id", IntegerType())
            .add("value_label", StringType())
            .add("value_id", IntegerType())
        ))
    ))

# Đọc toàn bộ dữ liệu từ Kafka (batch mode)
df_raw = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "KAFKA_BOOTSTRAP_SERVERS") \
    .option("subscribe", "KAFKA_TOPIC") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*")

# Lọc timestamp hợp lệ
df_parsed = df_parsed.filter(col("time_stamp").isNotNull() & (col("time_stamp") > 0)) \
                   .withColumn("time_stamp", to_timestamp(from_unixtime(col("time_stamp"))))

# Lọc các sự kiện checkout thành công
df_filtered = df_parsed.filter(col("collection") == "checkout_success")

# Explode cart_products và làm sạch giá
df_products = df_filtered.select(
    explode(col("cart_products")).alias("product")
).select(
    col("product.product_id").alias("product_id"),
    col("product.amount").alias("amount"),
    regexp_replace(col("product.price"), "[^0-9.]", "").cast(DoubleType()).alias("price"),
    col("product.currency").alias("currency")
).filter(col("price").isNotNull())

# Tính tổng doanh số và pivot
currency_list = ["MXN $", "Ft", "zł", "CLP", "SGD $", "AU $", "HKD $", "NZD $", "$", "£", "CAD $", "￥", "€"]
df_sales = df_products.withColumn(
    "total_sales",
    col("amount") * col("price")
).groupBy("product_id").pivot("currency", currency_list).sum("total_sales").na.fill(0.0)

# Đổi tên cột cho khớp với schema của bảng currency_totals
column_mapping = {
    "MXN $": "total_MXN_USD",
    "Ft": "total_Ft",
    "zł": "total_PLN",
    "CLP": "total_CLP",
    "SGD $": "total_SGD_USD",
    "AU $": "total_AU_USD",
    "HKD $": "total_HKD_USD",
    "NZD $": "total_NZD_USD",
    "$": "total_USD",
    "£": "total_GBP",
    "CAD $": "total_CAD_USD",
    "￥": "total_JPY",
    "€": "total_EUR"
}
df_final = df_sales.select(
    col("product_id").cast("long"),  # Đảm bảo product_id là BIGINT
    *[col(c).alias(column_mapping[c]).cast("decimal(18,2)") for c in currency_list]
)

# Lấy cấu hình MySQL
db_config = get_database_config()
mysql_config = db_config["mysql"]

# Ghi dữ liệu vào MySQL một lần
df_final.write \
    .format("jdbc") \
    .option("url", f"jdbc:mysql://{mysql_config.host}:{mysql_config.port}/{mysql_config.database}") \
    .option("dbtable", "currency_totals") \
    .option("user", mysql_config.user) \
    .option("password", mysql_config.password) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .mode("append") \
    .save()

logger.info("Successfully wrote all records to MySQL table 'currency_totals'")

# Dừng SparkSession
spark.stop()