import pg8000.native

host = 'nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com'
port = 5432
user = 'project_user_1'
password = 'UmaC43m32Zi6RW'
database = 'totesys'
schema = 'public'

# DB connection
con = pg8000.native.Connection(
    user,
    host=host,
    database=database,
    port=port,
    password=password
)
