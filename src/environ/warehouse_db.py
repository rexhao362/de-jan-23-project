from os import environ

env_warehouse_db = {
    "user": environ.get('WAREHOUSE_DB_USER'),
    "password": environ.get('WAREHOUSE_DB_PASSWORD'),
    "host": environ.get('WAREHOUSE_DB_HOST', 'localhost'),
    "port": environ.get('WAREHOUSE_DB_PORT', 5432),
    "database": environ.get('WAREHOUSE_DB_DATABASE'),
    "schema": environ.get('WAREHOUSE_DB_DATABASE_SCHEMA')
}
