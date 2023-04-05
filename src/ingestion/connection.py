import pg8000.native
import sys
sys.path.append('./src/ingestion')
sys.path.append('./src')
from gutils.secrets_manager import secrets_manager as sm  # noqa: E402


totesys_db = sm.get_secret_totesys_db_config()

# DB connection
con = pg8000.native.Connection(
    **totesys_db['credentials']
)

schema = totesys_db['schema']
