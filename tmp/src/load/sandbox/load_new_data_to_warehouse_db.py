import traceback
import pyarrow.parquet as pq
from pg8000.native import Connection
from src.environ.warehouse_db import env_warehouse_db as env
import src..load.format_functions as format_namespace
from src..load.populate_dim_currency import populate_dim_currency


def load_new_data_to_warehouse_db(data_path):
    print("load_new_data_to_warehouse_db():")

    input_data_descriptions = [
        {
            "table_name": "dim_currency",
            "file_name": "dim_currency.parquet"
        }
    ]

    # read tables from files and format data if not empty
    for input_data in input_data_descriptions:
        file_path = f'{data_path}/{ input_data["file_name"] }'
        table_name = input_data["table_name"]

        print(f'read_table("{file_path}")..')
        table = pq.read_table(file_path)
        data_frame = table.to_pandas()

        if data_frame.empty:
            print(f'Info: table "{table_name}" is empty')
            continue

        function_name = f'format_{input_data["table_name"]}_data'
        print(f'call {function_name}..')
        format_function = getattr(format_namespace, function_name)
        input_data["formatted_data"] = format_function(data_frame)                    

    # print("\nReady to populate warehouse DB")
    # for input_data in input_data_descriptions:
    #     formatted_data = input_data.get("formatted_data", None)
    #     if formatted_data:
    #         print(input_data["table_name"])
    #         print(input_data["formatted_data"], "\n")

    print(f'input_data={input_data}')

    with Connection(env["user"], password=env["password"], host=env["host"], port=env["port"], database=env["database"]) as connection:
        formatted_data = input_data_descriptions[0].get("formatted_data")
        if formatted_data:
            print(f'populate_dim_currency()..')
            populate_dim_currency(connection, formatted_data, env["schema"], "dim_currency")

    print("load_new_data_to_warehouse_db(): done")    

# if run directly
if __name__ == "__main__":
    try:
        load_new_data_to_warehouse_db("local/aws/s3/processed")

    except Exception as err:
        # msg = "".join(traceback.format_exception(type(err), err, err.__traceback__)) # full traceback, go with shorter one for now
        # msg = "".join( traceback.format_tb(err.__traceback__) ) # a shorter one, stil too much
        msg = f"\nError: {err}"
        exit(msg)  # replace with log() to CloudWatch

    finally:
        pass
        # clean up the process files
        #cleanup()