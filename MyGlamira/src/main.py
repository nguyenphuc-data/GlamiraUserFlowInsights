from database.mongodb_connect import MongoDBConnect, query_product_views
from database.schema_manager import create_mongodb_schema, validate_mongodb_schema
from config.database_config import get_database_config
from database.kafka_producer import KafkaMessageProducer

def main():
    configMongo = get_database_config()

    kafka_producer = KafkaMessageProducer(
        bootstrap_servers='localhost:9092',
        topic='product_views'
    )

    with MongoDBConnect(configMongo["mongodb"].uri, configMongo["mongodb"].db_name) as mongo_client:
        db = mongo_client.db

        # Nếu có logic schema
        create_mongodb_schema(db)
        validate_mongodb_schema(db)

        # Gửi dữ liệu sang Kafka
        total_records = query_product_views(db, kafka_producer)
        print(f"Finished processing {total_records} records")



if __name__ == "__main__":
   main()
