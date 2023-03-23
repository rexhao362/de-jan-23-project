import pyarrow.parquet as pq
import src.loading.format_functions as format_namespace

def load_new_data_to_warehouse_db():
    input_data_list = [
        {
            "table_name": "dim_currency",
            "file_name": "test/loading/input_files/dim_currency.parquet"
        }
    ]

    for input_data in input_data_list:
        try:
            print(f'read_table("{input_data["file_name"]}")')
            table = pq.read_table( input_data["file_name"] )
            data_frame = table.to_pandas()
            function_name = f'format_{input_data["table_name"]}_data'
            print(f'call {function_name}')
            format_function = getattr(format_namespace, function_name)
            input_data["formatted_data"] = format_function(data_frame)
            
        except Exception as e:
            exit( str(e) )

    print("\n")
    for input_data in input_data_list:
        print(input_data["table_name"])
        print(input_data["formatted_data"], "\n")

load_new_data_to_warehouse_db()