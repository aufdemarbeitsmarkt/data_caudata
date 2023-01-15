import os

from sqlalchemy import create_engine


try:
    # fall back to config credentials, i.e. assume we're working locally
    from config import DB_USERNAME, DB_PASSWORD, HOST

    os.environ['DB_USERNAME'] = DB_USERNAME
    os.environ['DB_PASSWORD'] = DB_PASSWORD
    os.environ['HOST'] = HOST

except ModuleNotFoundError:
    pass

DB_USERNAME = os.environ.get('DB_USERNAME')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
HOST = os.environ.get('HOST')

CONNECTION_STRING = f'postgresql+psycopg2://{DB_USERNAME}:{DB_PASSWORD}@{HOST}/postgres'

ENGINE = create_engine(CONNECTION_STRING)