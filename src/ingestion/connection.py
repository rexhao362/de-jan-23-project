import pg8000.native
from gutils.secrets_manager import secrets_manager as sm


totesys_db = sm.get_secret_totesys_db_config()

# DB connection
con = pg8000.native.Connection(
    user=totesys_db['credentials']['user'],
    host=totesys_db['credentials']['host'],
    database=totesys_db['credentials']['database'],
    port=totesys_db['credentials']['port'],
    password=totesys_db['credentials']['password']
)

schema = totesys_db['schema']
