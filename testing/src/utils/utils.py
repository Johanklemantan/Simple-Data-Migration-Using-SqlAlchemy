import os
from datetime import datetime, timedelta
from os import walk

import lxml.html as lh
import pandas as pd
import requests
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base


def clean_integer(value, default=None):
    if str(value) == "nan":
        return None

    if isinstance(value, int) or isinstance(value, float):
        return int(value)

    if value is None or value == "":
        return default

    value = value.rstrip("%")

    if not value.isdigit():
        return default

    return int(value)


def create_table_if_not_exists(
    db_connection_target,
    table_model,
):
    base = declarative_base()

    table_model.__table__.create(bind=db_connection_target, checkfirst=True)
    base.metadata.create_all(db_connection_target)

    session_maker = sqlalchemy.orm.sessionmaker()
    session_maker.configure(bind=db_connection_target)
    session = session_maker()

    session.commit()


def get_csv_data(file_name, is_full_path: bool = False):
    dir_path = os.path.dirname(os.path.realpath(__file__))

    if is_full_path:
        return pd.read_csv(file_name, low_memory=False)

    return pd.read_csv(dir_path + f"/../../data/{file_name}.csv", low_memory=False)


def create_session(conn):
    sess = sqlalchemy.orm.sessionmaker()
    sess.configure(bind=conn)
    return sess()


def safe_commit_session(session):
    session.commit()
    session.expunge_all()
    session.close()
