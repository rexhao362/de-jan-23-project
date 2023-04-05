def make_schema_table_name(schema_name, table_name):
    """
    Function to create PostgreSQL table name when using nop-public schema.

    Args:
        schemschema_namea: schema name (string).
        table_name: table name (string).

    Returns:
        String in format {schema_name}.{table_name}.

    Raises:
        ValueError if schema_name or table_name is empty
        TypeError if schema_name or table_name is not a string
    """
    
    def validate_str_param(name, value):
        if not type(value) is str:
            raise TypeError( f'{name} should be a string' )

        if not len(value):
            raise ValueError( f'{name} cannot be empty')

    validate_str_param("schema_name", schema_name)
    validate_str_param("table_name", table_name)

    return f'{schema_name}.{table_name}'