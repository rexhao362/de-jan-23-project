from num2words import num2words

currency_row_elements_description = [
    { "type": "int" },
    { "type": "str" },
    { "type": "str" }
]

currency_data_structure_description = {
    "type": "list",
    "elements": currency_row_elements_description
}


class ElementDescription:
    def __init__(self, element_type, position_ordinal):
        self.type = element_type
        self.position = position_ordinal

class RowElementsDescription:
    def __init__(self, row_elements_description):
        self.index = 0
        # self.type = 
        self.row_elements = []
        for position, element_description in enumerate(row_elements_description, 1):
            row_element = ElementDescription( \
                element_description["type"], \
                num2words(position, to = 'ordinal_num') \
            )
            self.row_elements.append(row_element)

    def __iter__(self):
        return self

    def __next__(self):
        if self.index == len(self.row_elements):
            raise StopIteration
        res = self.row_elements[self.index]
        self.index += 1
        return res

    def __len__(self):
        return len(self.row_elements)

class DatatStructureDescription:
    def __init__(self, data_structure_description):
        self.type = data_structure_description["type"]
        self.row_elements_description = RowElementsDescription( data_structure_description["elements"] )





def validate_data(data, data_structure_description = DatatStructureDescription(currency_data_structure_description), function_name=__name__):
    top_level_type = type(data).__name__
    if top_level_type != data_structure_description.type:
        error_message = f'{function_name}: data should be of type "{data_structure_description.type}" (got "{top_level_type}")'
        print('Error:', error_message)
        raise TypeError(error_message)

    ref_num_elements = len(data_structure_description.row_elements_description)

    for row in data:
        row_container_type = type(row).__name__
        if row_container_type != "list":
            error_message = f'{function_name}: each data element should be of type "list" (got "{row_container_type}")'
            print('Error:', error_message)
            raise TypeError(error_message)
        
        num_elements = len(row)
        if num_elements != ref_num_elements:
            error_message = f'{function_name}: each data element should be a list of {ref_num_elements} elements (got {num_elements})'
            print('Error:', error_message)
            raise ValueError(error_message)

        for element, element_description in zip(row, data_structure_description.row_elements_description):
            element_type = type(element).__name__
            ref_element_type = element_description.type
            if element_type != ref_element_type:
                element_position = element_description.position
                error_message = f'{function_name}: {element_position} element of each nested list should be of type "{ref_element_type}" (got "{element_type}")'
                print('Error:', error_message)
                raise TypeError(error_message)
        

def populate_dim_currency(connection, data, schema, table_name="dim_currency"):
    """
    Insert currency data into dim_currency table of the warehouse DB.
    Does not change the DB if data is an empty list.

    Args:
        connection: DB connection.
        data: currency data in format [ [currency_id, currency_code, currency_name], ... ]
            where
                currency_id is int
                currency_code is string
                currency_name is string

        schema: DB schema
        table_name: table name

    Returns:
        None.

    Raises:
        TypeError: if elements of data have invalid type
        ValueError: if data is not in required format 
    """
       
    validate_data(data)

    values = ",".join( [ f"('{record[0]}', '{record[1]}', '{record[2]}')" for record in data] )

    if values:
        res = connection.run( f"\
        INSERT INTO {schema}.{table_name}\
        (currency_id, currency_code, currency_name)\
        VALUES\
        {values}" )