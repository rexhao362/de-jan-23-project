import pyarrow as pa
import pyarrow.parquet as pq

class SQLType:
    # class variables
    supported_types = {
        "INT": pa.types.is_integer,
        "VARCHAR": pa.types.is_string
    }

    def __init__(self, type_name):
        uc_type_name = type_name.upper()
        if not uc_type_name in SQLType.supported_types:
            raise ValueError(f"unsupported SQL type: {type_name}")
        self.type_name = uc_type_name

    def __str__(self):
        return self.type_name

    def matches_arrow_type(self, arrow_type):
        return SQLType.supported_types[self.type_name](arrow_type)

sql_table_format = {
    "currency_id": "INT",
    "currency_code": "VARCHAR",
    "currency_name": "VARCHAR"
}

#table = pq.read_table("test/lambdas/load/input_files/dim_currency.parquet")
table = pa.table({
    "currency_name": ["Pounds", "Dollars", "Euros"],
    "currency_id": [1, 2, 3],
    "currency_code": ["GBP", "USD", "EUR"],
    "suxx": [48, 48, 48]
})

print("original table:\n", table, "\n")

filtered_table = table.select( [column_name for column_name in sql_table_format] )

# validate
for column_name, sql_value_type in sql_table_format.items():
    #print(f"{column_name}: {value_type}")
    column_type = filtered_table[column_name].type
    if not SQLType(sql_value_type).matches_arrow_type( column_type ):
        exit(f'column_type={column_type} is different from {sql_value_type}')
    #print(column_type, "\n")
    #type_match = sql_to_pyarrow_type_map[value_type](column_type)
    #print(f'type_match={type_match}')

rows = filtered_table.to_pylist()

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