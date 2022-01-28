import sqlalchemy
from pandas import Series
from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base

from src.config.database import get_db_connection
from src.utils.utils import (
    clean_integer,
    create_table_if_not_exists,
    get_csv_data,
)

Base = declarative_base()


class Province(Base):
    __tablename__ = "mst_province"
    id = Column(Integer, primary_key=True)
    region_id = Column(Integer, nullable=False)
    nama_provinsi = Column(String(length=255))


def sync_entity(new_entity: Province, fetched_data: Series):
    new_entity.id = fetched_data["id"]
    new_entity.region_id = clean_integer(fetched_data["region_id"])
    new_entity.nama_provinsi = fetched_data["nama_provinsi"]


def create_table_mst_province():
    print("Creating table mst_province \n")
    db_connection_sink = get_db_connection()

    create_table_if_not_exists(
        db_connection_target=db_connection_sink, table_model=Province
    )

    print("Table mst_province succesfully created")
    print("Inserting content to table.. \n")

    df = get_csv_data(Province.__tablename__)

    session_maker = sqlalchemy.orm.sessionmaker()
    session_maker.configure(bind=db_connection_sink)
    session = session_maker()

    for index, row in df.iterrows():
        existing_data = (
            session.query(Province).filter(
                Province.id == row["id"]).one_or_none()
        )

        if existing_data:
            continue

        new_data = Province()
        sync_entity(new_data, row)

        session.add(new_data)

        session.commit()

    print("Berhazeil :)")
