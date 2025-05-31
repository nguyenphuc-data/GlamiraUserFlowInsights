import os
from dataclasses import dataclass
from dotenv import load_dotenv
from typing import Dict


class DatabaseConfig:
   def validate(self) -> None:
       for key, value in self.__dict__.items():
           if value is None:
               raise ValueError(f"Missing config for {key}")


@dataclass
class MongoDBConfig(DatabaseConfig):
   uri: str
   db_name: str

@dataclass
class MySQLConfig(DatabaseConfig):
    host : str
    user : str
    password : str
    database : str
    port : int

def get_database_config() -> Dict[str, DatabaseConfig]:
   load_dotenv()
   config = {
       "mongodb": MongoDBConfig(
           uri=os.getenv("MONGO_URI"),
           db_name=os.getenv("MONGO_DB_NAME")
       ),
       "mysql": MySQLConfig(
           host=os.getenv("MYSQL_HOST"),
           port=int(os.getenv("MYSQL_PORT")),
           user=os.getenv("MYSQL_USER"),
           password=os.getenv("MYSQL_PASSWORD"),
           database=os.getenv("MYSQL_DATABASE")
       )
   }
   for db, setting in config.items():
       setting.validate()
   return config


db_config = get_database_config()
