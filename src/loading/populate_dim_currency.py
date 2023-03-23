def populate_dim_currency(connection, input_currency_data, schema, table_name="dim_currency"):
    values = ",".join( [ f"('{record[0]}', '{record[1]}', '{record[2]}')" for record in input_currency_data] )

    res = connection.run( f"\
    INSERT INTO {schema}.{table_name}\
    (currency_id, currency_code, currency_name)\
    VALUES\
    {values}" )