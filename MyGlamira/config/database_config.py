import os
from dataclasses import dataclass

from dotenv import load_dotenv
from typing import Dict


class DatabaseConfig():
    def validate(self) -> None:
        for key, value in self.__dict__.items():
            if value is None:
                raise ValueError(f"----------Missing config for {key}-------------")

@dataclass
class MongoDBConfig(DatabaseConfig):
    uri : str
    db_name : str

def get_database_config() -> Dict[str,DatabaseConfig]:
    load_dotenv()
    config = {
        "mongodb" :  MongoDBConfig(
            uri = os.getenv("MONGO_URI"),
            db_name = os.getenv("MONGO_DB_NAME")
        )
    }

    for db, setting in config.items():
        print(type(setting))
        setting.validate()

    return config


db_config = get_database_config()
print(db_config)
