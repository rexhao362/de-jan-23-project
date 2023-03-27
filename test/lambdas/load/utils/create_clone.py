import sys
import pandas

# for arg in sys.argv:
#     print(f'arg={arg}')

input_file = sys.argv[1]
output_file = sys.argv[2]

num_rows = int(sys.argv[3])

print(f'read_parquet({input_file})..')
print(f'and to_parquet({output_file})..')
print(f'and preserve {num_rows} rows')


try:
    data_frame = pandas.read_parquet(input_file)
    data_frame.iloc[0:num_rows].to_parquet(output_file)
except Exception as e:
    print("Ex:", e)