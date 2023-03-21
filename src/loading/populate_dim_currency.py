def populate_dim_currency(connection, input_currency_data): # [ [r1, r2, r3] ]
    values = ",".join( [ f"('{record[0]}', '{record[1]}', '{record[2]}')" for record in input_currency_data] )

    res = connection.run( f"\
    INSERT INTO dim_currency\
    (currency_id, currency_code, currency_name)\
    VALUES\
    {values}" )