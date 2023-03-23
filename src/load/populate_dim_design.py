def populate_dim_design(connection, data, schema, table_name="dim_design"):
    values = ",".join( [ f"('{record[0]}', '{record[1]}', '{record[2]}', '{record[3]}')" for record in data] )
    
    res = connection.run( f"\
    INSERT INTO {schema}.{table_name}\
    (design_id, design_name, file_location, file_name)\
    VALUES\
    {values}" )