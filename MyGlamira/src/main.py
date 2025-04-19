from database.mongodb_connect import MongoDBConnect, query_product_views
from database.schema_manager import create_mongodb_schema, validate_mongodb_schema
from config.database_config import get_database_config


def main():
    configMongo = get_database_config()
    with MongoDBConnect(configMongo["mongodb"].uri, configMongo["mongodb"].db_name) as mongo_client:
        db = mongo_client.db
        # Ensure schema exists and validate
        create_mongodb_schema(db)
        validate_mongodb_schema(db)

        # Query and display documents
        documents = query_product_views(db)
        print("Retrieved documents:")
        for doc in documents:
            print(doc)


if __name__ == "__main__":
    main()