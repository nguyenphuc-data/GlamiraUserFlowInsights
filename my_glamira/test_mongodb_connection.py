import pymongo
from pymongo.errors import ConnectionFailure, OperationFailure
import time

def test_mongodb_connection():
    """
    Test connection to MongoDB using a hardcoded connection string.
    
    Returns:
        bool: True if connection successful, False otherwise
    """
    # Define your MongoDB connection string here
    connection_string = "mongodb://nguyenphuc:123456@localhost:27017/"
    
    # You can specify a specific database if needed
    database_name = "Glamira"
    
    print(f"Attempting to connect to MongoDB with connection string: {connection_string}")
    
    try:
        # Create a MongoDB client with a timeout
        start_time = time.time()
        client = pymongo.MongoClient(connection_string, serverSelectionTimeoutMS=5000)
        
        # The ismaster command is cheap and does not require auth
        client.admin.command('ismaster')
        
        # Calculate connection time
        connection_time = time.time() - start_time
        
        # If we get here, the connection was successful
        print(f"MongoDB connection successful! Connected in {connection_time:.2f} seconds.")
        
        # Test access to the specific database if provided
        if database_name:
            db = client[database_name]
            collections = db.list_collection_names()
            print(f"\nCollections in database '{database_name}':")
            if collections:
                for collection in collections:
                    print(f"- {collection}")
                    # Get count of documents in this collection
                    count = db[collection].count_documents({})
                    print(f"  • Contains {count} documents")
            else:
                print("No collections found in this database.")
        
        # List all available databases
        print("\nAll available databases:")
        database_names = client.list_database_names()
        for db_name in database_names:
            print(f"- {db_name}")
            
        # Close the connection
        client.close()
        return True
        
    except ConnectionFailure as e:
        print(f"MongoDB connection failed: Could not connect to server. Error: {e}")
        print("\nPossible issues:")
        print("1. The server address or port is incorrect")
        print("2. The server is not running")
        print("3. Network connectivity issues or firewall blocking the connection")
        return False
    except OperationFailure as e:
        if "Authentication failed" in str(e):
            print(f"MongoDB connection failed: Authentication failed. Error: {e}")
            print("\nPossible issues:")
            print("1. Username or password is incorrect")
            print("2. User does not have access to the specified database")
            print("3. Authentication database is incorrect (should be specified in the connection string)")
        else:
            print(f"MongoDB operation failed. Error: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        print(f"Error type: {type(e).__name__}")
        return False

if __name__ == "__main__":
    # Test the connection
    test_mongodb_connection()
    
    # Keep console window open if run by double-clicking
    input("\nPress Enter to exit...")