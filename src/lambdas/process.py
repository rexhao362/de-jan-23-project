import pandas as pd
import json

# injest json
# process into parquet
# upload to s3 / write to local
def load_file_from_s3():
    pass

def load_file_from_local(filepath):
    json_data = open(filepath)
    data = json.load(json_data)
    json_data.close()
    return data

def process():
    pass

def upload_to_s3():
    pass

def save_to_local():
    pass

def main():
    table1 = 'test/json_files/process_test_1.json'
    table1_data = load_file_from_local(table1)
    print(table1_data)



if __name__ == "__main__":
    main()