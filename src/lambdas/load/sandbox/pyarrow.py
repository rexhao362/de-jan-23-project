import pyarrow.parquet as pq

table = pq.read_table("test/load/input_files/dim_currency.parquet")

columns = table.column_names

# print(f'Columns: {table.column_names}')

col_currency_code = table["currency_code"]
# print(f'{col_currency_code}')

# print(f'{col_currency_code[1]}')

for row_index in range(len(columns)):
    data_row = [ str( col_currency_code[row_index] ) ]
    print(data_row)

#     data_row = [ int(table.field(0)[index]), table.field(1)[index], table.field(2)[index] ]
#     # for column in table:
#     #     data_row.append(column[index])

#     print(f'{data_row}')