import pymongo
import json
from bson.objectid import ObjectId
import pandas as pd
from datetime import datetime
import os

# Function to connect to MongoDB
def connect_to_mongodb(connection_string, db_name):
    client = pymongo.MongoClient(connection_string)
    db = client[db_name]
    return db

# Function to extract specific fields from a collection
def extract_data_from_collection(db, collection_name, output_file):
    collection = db[collection_name]
    
    # Define fields to extract
    fields_to_extract = {
        "_id": 1, 
        "device_id": 1, 
        "time_stamp": 1, 
        "current_url": 1, 
        "referrer_url": 1, 
        "email_address": 1, 
        "collection": 1, 
        "product_id": 1, 
        "option": 1
    }
    
    # Query the collection
    cursor = collection.find({}, fields_to_extract)
    
    # Process documents
    extracted_data = []
    for doc in cursor:
        # Convert ObjectId to string
        if "_id" in doc and isinstance(doc["_id"], ObjectId):
            doc["_id"] = str(doc["_id"])
        
        extracted_data.append(doc)
    
    # Export as JSON
    if extracted_data:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(extracted_data, f, ensure_ascii=False, indent=4)
        
        print(f"Extracted {len(extracted_data)} records to {output_file}")
        return extracted_data
    else:
        print("No data found matching the criteria")
        return None

# Function to extract data from a BSON file directly (without MongoDB)
def extract_from_bson_file(bson_file_path, output_file):
    import bson
    
    # Check if file exists
    if not os.path.exists(bson_file_path):
        print(f"File {bson_file_path} not found")
        return None
    
    # Read BSON file
    with open(bson_file_path, 'rb') as f:
        data = bson.decode_all(f.read())
    
    # Extract required fields
    extracted_data = []
    for doc in data:
        extracted_doc = {}
        
        # Extract each specified field if it exists
        for field in ["_id", "device_id", "time_stamp", "current_url", 
                     "referrer_url", "email_address", "collection", "product_id", "option"]:
            if field in doc:
                # Handle ObjectId conversion
                if field == "_id" and isinstance(doc[field], ObjectId):
                    extracted_doc[field] = str(doc[field])
                else:
                    extracted_doc[field] = doc[field]
        
        extracted_data.append(extracted_doc)
    
    # Export as JSON
    if extracted_data:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(extracted_data, f, ensure_ascii=False, indent=4)
        
        print(f"Extracted {len(extracted_data)} records to {output_file}")
        return extracted_data
    else:
        print("No data found in the BSON file")
        return None

# Function to handle datetime serialization for JSON
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)

# Main execution
if __name__ == "__main__":

    connection_string = "mongodb://nguyenphuc:123456@localhost:27017/"
    db_name = "Glamira"
    collection_name = "summary"
    output_file = "extracted_data.json"
    db = connect_to_mongodb(connection_string, db_name)
    data = extract_data_from_collection(db, collection_name, output_file)
    
    if data is not None:
        print("Data extraction completed successfully")
        print(f"Total records extracted: {len(data)}")
        if len(data) > 0:
            print("Sample of first record structure:")
            print(json.dumps(data[0], indent=2, ensure_ascii=False))