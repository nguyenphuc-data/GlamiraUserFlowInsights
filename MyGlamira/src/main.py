from database.mongodb_connect import MongoDBConnect, query_product_views
from database.schema_manager import create_mongodb_schema, validate_mongodb_schema
from config.database_config import get_database_config


def main():
    # Load configuration
    configMongo = get_database_config()

    # Connect to MongoDB
    with MongoDBConnect(configMongo["mongodb"].uri, configMongo["mongodb"].db_name) as mongo_client:
        db = mongo_client.db

        # Ensure schema exists and validate
        create_mongodb_schema(db)
        validate_mongodb_schema(db)

        # Process all documents and log progress
        total_records = query_product_views(db)
        print(f"Finished processing {total_records} records")


if __name__ == "__main__":
    main()