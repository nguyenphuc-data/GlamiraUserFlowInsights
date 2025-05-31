from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from config.database_config import get_database_config
from bson import ObjectId

def clean_document(doc):
    # Convert ObjectId và các kiểu không JSON-serializable về chuỗi
    for key, value in doc.items():
        if isinstance(value, ObjectId):
            doc[key] = str(value)
    return doc

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
           print(f"Connected to MongoDB: {self.db_name}")
           return self.db
       except ConnectionFailure as e:
           raise Exception(f"Failed to connect to MongoDB: {e}") from e


   def close(self):
       if self.client:
           self.client.close()
           print("MongoDB connection closed")


   def __enter__(self):
       self.connect()
       return self


   def __exit__(self, exc_type, exc_val, exc_tb):
       self.close()

def query_product_views(db, kafka_producer=None):
   projection = {
       "_id": 1,
       "time_stamp": 1,
       "current_url": 1,
       "referrer_url": 1,
       "collection": 1,
       "cart_products": 1
   }
   cursor = db.summary.find({}, projection)

   records_processed = 0

   try:
       for doc in cursor:
           records_processed += 1

           # Send to Kafka if producer is provided
           if kafka_producer:
               cleaned_doc = clean_document(doc)
               kafka_producer.send(cleaned_doc)

           if records_processed % 500000 == 0:
               print(f"Processed {records_processed} records")
   finally:
       cursor.close()

   print(f"Total records processed: {records_processed}")
   if kafka_producer:
       kafka_producer.flush()

   return records_processed


