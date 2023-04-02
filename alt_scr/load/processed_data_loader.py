import logging
from gutils.environ import is_production_environ
from db_schema import mvp_database_schema as default_database_schema


logger = logging.getLogger('TEST_Q2_LOAD')
logger.setLevel(logging.INFO)

def load_processed_data(event, context):
    is_production = is_production_environ()

    logger.info("Loadinbg data into Warehouse DB")
    logger.info( f'is_production={is_production}' )
    logger.info( f'default_database_schema={ dir(default_database_schema) }' )

# standalone execution
if __name__ == '__main__':
    import sys
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)

    load_processed_data({}, {})