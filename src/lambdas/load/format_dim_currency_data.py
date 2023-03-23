default_column_names = [
    "currency_id",
    "currency_code",
    "currency_name"
]

def format_dim_currency_data(data_frame, column_names=default_column_names):
    """
    Returns: a list of lists (each represent values of the table's row)
    Raises exception in case of error when reading parquet file, missing columns
    """
    # check that all columns necessary for the table are present
    for column_name in column_names:
        if not column_name in data_frame.columns:
            raise Exception( f'no "{column_name}" label found in "{parquet_file_name}"' )

    return [
            [ row[column_name] for column_name in column_names ]
                for index, row in data_frame.iterrows()
        ]