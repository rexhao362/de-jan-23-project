def populate_dim_staff(connection, data, table_name="dim_staff"):
    values = ",".join( [ f"('{record[0]}', '{record[1]}', '{record[2]}', '{record[3]}', '{record[4]}', '{record[5]}')" for record in data] )
    # record = data[0]
    # values = f"('{record[0]}', '{record[1]}', '{record[2]}', '{record[3]}', '{record[4]}', '{record[5]}')"

    res = connection.run( f"\
    INSERT INTO {table_name}\
    (staff_id, first_name, last_name, department_name, location, email_address)\
    VALUES\
    {values}" )