import json
import os

import pandas as pd
from sqlalchemy import create_engine

dir_path = os.path.dirname(os.path.realpath(__file__))

sink = json.load(open(dir_path + "/../../config/data-config.json"))


def get_connection_string():
    data = sink

    return (
        data.get("driver")
        + data.get("username")
        + ":"
        + data.get("password")
        + "@"
        + data.get("host")
        + ":"
        + data.get("port")
        + "/"
        + data.get("database")
    )


def get_db_connection():
    db_connection = create_engine(
        get_connection_string(), pool_size=30, max_overflow=10
    )
    return db_connection
