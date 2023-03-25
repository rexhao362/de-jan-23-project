import pyarrow as pa
import pyarrow.parquet as pq

default_column_names = [
    "currency_id",
    "currency_code",
    "currency_name"
]

#table = pq.read_table("test/lambdas/load/input_files/dim_currency.parquet")
table = pa.table({
    "currency_name": ["Pounds", "Dollars", "Euros"],
    "currency_id": [1, 2, 3],
    "currency_code": ["GBP", "USD", "EUR"],
    "suxx": [48, 48, 48]
})

print("original table:\n", table, "\n")

# for table_name in table.column_names:
#     if not table_name in default_column_names:
#         table.drop(table_name)


rows = table.select( [column_name for column_name in default_column_names] ).to_pylist()

print("filtered table\n")
columns = ' '.join(table.column_names)
print(columns)

rows_strs = []
for row_number, row in enumerate(rows):
    #print( ' '.join( [str(value) for value in row.values()] ) )
    row_str = "(" + ', '.join( [ f'{element}' for element in row.values() ] ) + ")"
    rows_strs.append(row_str)

print( ', '.join(rows_strs) )


# columns = table.column_names

# # print(f'Columns: {table.column_names}')

# col_currency_code = table["currency_code"]
# print(f'{col_currency_code}')

# # print(f'{col_currency_code[1]}')

# for row_index in range(len(columns)):
#     data_row = [ str( col_currency_code[row_index] ) ]
#     print(data_row)

# #     data_row = [ int(table.field(0)[index]), table.field(1)[index], table.field(2)[index] ]
# #     # for column in table:
# #     #     data_row.append(column[index])

# #     print(f'{data_row}')