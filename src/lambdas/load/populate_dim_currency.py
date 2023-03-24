def validate_data(data, function_name="populate_dim_currency"):
    if not type(data) is list:
        error_message = f'{function_name}: data should be a list (got {type(data)})'
        raise TypeError(error_message)

    for record in data:
        if not type(record) is list:
            error_message = f'{function_name}: each data element should be a list (got {type(record)})'
            raise TypeError(error_message)
        
        num_elements = len(record)
        if num_elements != 3:
            error_message = f'{function_name}: each data element should be a list of 3 elements (got {num_elements})'
            raise ValueError()

        element = record[0]
        if type(element) != int:
            error_message = f'{function_name}: first element of each nested list should be an integer (got {type(element)})'
            raise TypeError(error_message)

        element = record[1]
        if type(element) != str:
            error_message = f'{function_name}: second element of each nested list should be a string (got {type(element)})'
            raise TypeError(error_message)

        element = record[2]
        if type(element) != str:
            error_message = f'{function_name}: third element of each nested list should be a string (got {type(element)})'
            raise TypeError(error_message)
        

def populate_dim_currency(connection, data, schema, table_name="dim_currency"):
    """
    Insert currency data into dim_currency table of the warehouse DB.
    Does not change the DB if data is an empty list.

    Args:
        connection: DB connection.
        data: currency data in format [ [currency_id, currency_code, currency_name], ... ]
            where
                currency_id is int
                currency_code is string
                currency_name is string

        schema: DB schema
        table_name: table name

    Returns:
        None.

    Raises:
        TypeError: if elements of data have invalid type
        ValueError: if data is not in required format 
    """
    function_name = populate_dim_currency.__name__      
    validate_data(data, function_name)

    values = ",".join( [ f"('{record[0]}', '{record[1]}', '{record[2]}')" for record in data] )

    if values:
        res = connection.run( f"\
        INSERT INTO {schema}.{table_name}\
        (currency_id, currency_code, currency_name)\
        VALUES\
        {values}" )