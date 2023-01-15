import os

from sqlalchemy import create_engine

from config import *


if __name__ == '__main__':
    # fall back to config credentials, i.e. assume we're working locally
    DB_USERNAME = os.environ.get('DB_USERNAME', config.DB_USERNAME)
    DB_PASSWORD = os.environ.get('DB_PASSWORD', config.DB_PASSWORD)
    HOST = os.environ.get('HOST', config.HOST)

    CONNECTION_STRING = f'postgresql+psycopg2://{DB_USERNAME}:{DB_PASSWORD}@{HOST}/postgres'

    ENGINE = create_engine(CONNECTION_STRING)