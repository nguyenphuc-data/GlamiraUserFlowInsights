from pymongo import MongoClient
from pymongo.errors import CollectionInvalid

def create_mongodb_schema(db):
    if "summary" not in db.list_collection_names():
        try:
            db.create_collection("summary", validator={
                "$jsonSchema": {
                    "bsonType": "object",
                    "required": ["_id", "time_stamp", "current_url", "collection"],
                    "properties": {
                        "_id": {"bsonType": ["objectId", "string"]},
                        "time_stamp": {"bsonType": ["int", "long"]},
                        "ip": {"bsonType": ["string", "null"]},
                        "user_agent": {"bsonType": ["string", "null"]},
                        "resolution": {"bsonType": ["string", "null"]},
                        "user_id_db": {"bsonType": ["string", "null"]},
                        "device_id": {"bsonType": ["string", "null"]},
                        "api_version": {"bsonType": ["string", "null"]},
                        "store_id": {"bsonType": ["string", "null"]},
                        "local_time": {"bsonType": ["string", "null"]},
                        "show_recommendation": {"bsonType": ["string", "bool", "null"]},
                        "current_url": {"bsonType": "string"},
                        "referrer_url": {"bsonType": ["string", "null"]},
                        "email_address": {"bsonType": ["string", "null"]},
                        "recommendation": {"bsonType": ["bool", "null"]},
                        "utm_source": {"bsonType": ["bool", "string", "null"]},
                        "utm_medium": {"bsonType": ["bool", "string", "null"]},
                        "collection": {"bsonType": "string"},
                        "product_id": {"bsonType": ["string", "null"]},  # Giữ để tương thích dữ liệu cũ
                        "option": {
                            "bsonType": ["array", "null"],
                            "items": {
                                "bsonType": "object",
                                "properties": {
                                    "option_label": {"bsonType": ["string", "null"]},
                                    "option_id": {"bsonType": ["string", "int", "null"]},
                                    "value_label": {"bsonType": ["string", "null"]},
                                    "value_id": {"bsonType": ["string", "int", "null"]}
                                }
                            }
                        },
                        "cart_products": {
                            "bsonType": ["array", "null"],
                            "items": {
                                "bsonType": "object",
                                "required": ["product_id", "amount", "price", "currency"],
                                "properties": {
                                    "product_id": {"bsonType": ["int", "string"]},
                                    "amount": {"bsonType": ["int"]},
                                    "price": {"bsonType": ["string"]},  # Giữ string vì giá trị như "30,00"
                                    "currency": {"bsonType": ["string"]},
                                    "option": {
                                        "bsonType": ["array", "null"],
                                        "items": {
                                            "bsonType": "object",
                                            "properties": {
                                                "option_label": {"bsonType": ["string", "null"]},
                                                "option_id": {"bsonType": ["int", "string", "null"]},
                                                "value_label": {"bsonType": ["string", "null"]},
                                                "value_id": {"bsonType": ["int", "string", "null"]}
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            })
            db.summary.create_index("product_id")
            print("Created summary collection with schema")
        except CollectionInvalid:
            print("summary collection already exists")
    else:
        print("summary collection already exists, skipping creation")

def validate_mongodb_schema(db):
    collections = db.list_collection_names()
    print("Collections:", collections)
    if "summary" not in collections:
        raise ValueError("Missing summary collection in MongoDB")
    if db.summary.find_one() is None:
        print("Warning: summary collection is empty")
    else:
        print("summary collection contains documents")
    print("Validated schema for summary collection")