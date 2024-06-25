import json

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import sqlalchemy as db

engine = db.create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/postgres')

Base = declarative_base()


class Event(Base):
    __tablename__ = 'event'
    id = db.Column(db.UUID, nullable=False, primary_key=True)
    lut = db.Column(db.BigInteger, nullable=False, primary_key=True)
    name = db.Column(db.String(30), nullable=False)
    longitude = db.Column(db.DOUBLE_PRECISION, nullable=False)
    latitude = db.Column(db.DOUBLE_PRECISION, nullable=False)
    description = db.Column(db.String(500), nullable=False)

    def to_json(self):
        return {
            'id': self.id,
            'lut': self.lut,
            'name': self.name,
            'longitude': self.longitude,
            'latitude': self.latitude,
            'description': self.description
        }


Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()
