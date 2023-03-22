import pandas

column_names = [
    "currency_id",
    "currency_code",
    "currency_name"
]

def format_dim_currency_data(parquet_file_name):
    output_data = []
    
    data_frame = pandas.read_parquet(parquet_file_name)

    for index, row in data_frame.iterrows():
        output_data.append( \
            [ row[column_name] for column_name in column_names ] \
        )

    return output_data

