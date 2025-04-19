import logging
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from config.database_config import get_database_config

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MongoDBConnect:
    def __init__(self, mongo_uri, db_name):
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.client = None
        self.db = None

    def connect(self):
        try:
            self.client = MongoClient(self.mongo_uri)
            self.client.server_info()  # Test connection
            self.db = self.client[self.db_name]
            logger.info(f"------Connected to MongoDB: {self.db_name}----------")
            return self.db
        except ConnectionFailure as e:
            raise Exception(f"--------Failed to connect MongoDB: {e}---------")

    def disconnect(self):
        if self.client:
            self.client.close()
            logger.info(f"------Disconnected from MongoDB: {self.db_name}----------")

    def get_collection_data(self, collection_name, query=None, projection=None, limit=10):
        """Truy vấn dữ liệu BSON từ một collection"""
        try:
            collection = self.db[collection_name]
            if query is None:
                query = {}
            if projection is None:
                projection = {}  # Trả về tất cả các trường nếu không chỉ định
            data = collection.find(query, projection).limit(limit)
            return list(data)
        except Exception as e:
            logger.error(f"Error querying collection {collection_name}: {e}")
            return []

    def process_bson_data(self, collection_name, query=None, fields=None):
        """Xử lý dữ liệu BSON, chỉ giữ các trường cần thiết"""
        projection = {field: 1 for field in fields} if fields else None
        projection["_id"] = 0  # Loại bỏ trường _id
        data = self.get_collection_data(collection_name, query, projection)
        return data

def main():
    try:
        configMongo = get_database_config()
        mongo_instance = MongoDBConnect(configMongo["mongodb"].uri, configMongo["mongodb"].db_name)
        db = mongo_instance.connect()
        
        # Truy vấn dữ liệu từ collection 'view_product_detail'
        fields = ["time_stamp", "user_id_db", "product_id", "option", "current_url"]
        data = mongo_instance.process_bson_data(
            collection_name="view_product_detail",
            fields=fields
        )
        
        # In dữ liệu mẫu
        for doc in data:
            logger.info(f"Document: {doc}")
        
    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        if 'mongo_instance' in locals():
            mongo_instance.disconnect()

if __name__ == "__main__":
    main()