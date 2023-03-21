import os
import json
from decimal import Decimal
from ingestion_function.connection import con
from datetime import datetime

# Create data dir
if not os.path.exists('./ingestion-function/data'):
    os.makedirs('./ingestion-function/data')


# Queries database for table names
def get_table_names():
    table_names = con.run(
        'SELECT table_name, table_schema FROM information_schema.tables')
    #  print(table_names)
    return [item[0] for item in table_names if item[1] == 'public']


# Extracts column names for current table
def get_headers():
    return [c['name'] for c in con.columns]


# Extracts list of table data
def get_table_data(table):
    list = con.run(f'SELECT * FROM {table}')
    list.insert(0, get_headers())
    return list

# Writes dictionary of table to file
def data_ingestion():
    for table in get_table_names():
        list = get_table_data(table)
        for row in list:
            for i in range(len(row)):
                if isinstance(row[i], datetime):
                    row[i] = row[i].strftime('%Y-%m-%dT%H:%M:%S.%f')
                elif isinstance(row[i], Decimal):
                    row[i] = float(row[i])

        dict = {
            'table_name': table,
            'headers': list[0],
            'data': list[1:]
        }

        with open(f'./ingestion-function/data/{table}.json', 'w') as f:
            f.write(json.dumps(dict))


# data_ingestion()


def upload_to_s3():
    pass


def store_last_updated():
    pass
# Counterparty data


# def get_counterparty():
#     list_counterparty = []
#     counterparties = con.run('SELECT * FROM counterparty')

#     for counterparty in counterparties:
#         counterparty[5] = counterparty[5].strftime('%Y-%m-%dT%H:%M:%S.%f')
#         counterparty[6] = counterparty[6].strftime('%Y-%m-%dT%H:%M:%S.%f')
#         list_counterparty.append(counterparty)

#     dict_counterparty = {
#         'Table_name': 'counterparty',
#         'Headers': get_headers(),
#         'Data': list_counterparty
#     }

#     with open('./ingestion-function/data/counterparty.json', 'w') as file:
#         file.write(json.dumps(dict_counterparty))


# # Currency data

# def get_currency():
#     headers = con.run("select * from currency where false")

#     list_currency = []
#     currencies = con.run('SELECT * FROM currency')

#     for currency in currencies:
#         currency[2] = currency[2].strftime('%Y-%m-%dT%H:%M:%S.%f')
#         currency[3] = currency[3].strftime('%Y-%m-%dT%H:%M:%S.%f')
#         list_currency.append(currency)

#     dict_currency = {
#         'Table_name': 'currency',
#         'Headers': get_headers(),
#         'Data': list_currency
#     }

#     # 'created_at': currency[2].strftime('%Y-%m-%dT%H:%M:%S.%f'),
#     # 'last_updated': currency[3].strftime('%Y-%m-%dT%H:%M:%S.%f')

#     with open('./ingestion-function/data/currency.json', 'w') as file:
#         file.write(json.dumps(dict_currency))


# # Departments data


# def get_departments():
#     list_department = []
#     departments = con.run('SELECT * FROM department')

#     for department in departments:
#         department[4] = department[4].strftime('%Y-%m-%dT%H:%M:%S.%f')
#         department[5] = department[5].strftime('%Y-%m-%dT%H:%M:%S.%f')
#         list_department.append(department)

#     dict_department = {
#         'Table_name': 'department',
#         'Headers': get_headers(),
#         'Data': list_department
#     }

#     with open('./ingestion-function/data/departments.json', 'w') as file:
#         file.write(json.dumps(dict_department))


# #  Design data

# def get_design():
#     list_design = []
#     designs = con.run('SELECT * FROM design')

#     for design in designs:
#         print(design[2])
#         design[1] = design[1].strftime('%Y-%m-%dT%H:%M:%S.%f')
#         design[2] = design[2].strftime('%Y-%m-%dT%H:%M:%S.%f')
#         list_design.append(design)

#     dict_design = {
#         'Table_name': 'design',
#         'Headers': get_headers(),
#         'Data': list_design
#     }

#     with open('./ingestion-function/data/design.json', 'w') as file:
#         file.write(json.dumps(dict_design))


# #  Staff data
# def get_staff():
#     list_staff = []
#     staff = con.run('SELECT * FROM staff')

#     for person in staff:
#         person[5] = person[5].strftime('%Y-%m-%dT%H:%M:%S.%f')
#         person[6] = person[6].strftime('%Y-%m-%dT%H:%M:%S.%f')
#         list_staff.append(person)

#     dict_staff = {
#         'Table_name': 'staff',
#         'Headers': get_headers(),
#         'Data': list_staff
#     }

#     with open('./ingestion-function/data/staff.json', 'w') as file:
#         file.write(json.dumps(dict_staff))


# #  Sales_order data

# def get_sales_order():
#     list_sales_order = []
#     orders = con.run('SELECT * FROM sales_order')

#     for sales_order in orders:
#         sales_order[1] = sales_order[1].strftime('%Y-%m-%dT%H:%M:%S.%f')
#         sales_order[2] = sales_order[2].strftime('%Y-%m-%dT%H:%M:%S.%f')
#         list_sales_order.append(sales_order)

#     dict_sales_order = {
#         'Table_name': 'sales_order',
#         'Headers': get_headers(),
#         'Data': list_sales_order
#     }

#     with ('./ingestion-function/data/sales_order.json', 'w') as file:
#         file.write(json.dumps(dict_sales_order))


# # Address data

# def get_address():
#     list_address = []
#     addresses = con.run('SELECT * FROM address')

#     for address in addresses:
#         address[8] = address[8].strftime('%Y-%m-%dT%H:%M:%S.%f')
#         address[9] = address[9].strftime('%Y-%m-%dT%H:%M:%S.%f')
#         list_address.append(address)

#     dict_address = {
#         'Table_name': 'address',
#         'Headers': get_headers(),
#         'Data': list_address
#     }

#     with ('./ingestion-function/data/address.json', 'w') as file:
#         file.write(json.dumps(dict_address))


# # Payment data

# def get_payment():

#     list_payment = []
#     payments = con.run('SELECT * FROM payment')

#     for payment in payments:
#         payment[1] = payment[1].strftime('%Y-%m-%dT%H:%M:%S.%f')
#         payment[2] = payment[2].strftime('%Y-%m-%dT%H:%M:%S.%f')
#         list_payment.append(payment)

#     dict_payment = {
#         'Table_name': 'payment',
#         'Headers': get_headers(),
#         'Data': list_payment
#     }

#     with ('./ingestion-function/data/payment.json', 'w') as file:
#         file.write(json.dumps(dict_payment))


# #  Purchase_order data

# def get_purchase_order():

#     list_purchase_order = []
#     purchases = con.run('SELECT * FROM purchase_order')

#     for purchase_order in purchases:
#         purchase_order[1] = purchase_order[1].strftime('%Y-%m-%dT%H:%M:%S.%f')
#         purchase_order[2] = purchase_order[2].strftime('%Y-%m-%dT%H:%M:%S.%f')
#         list_purchase_order.append(purchase_order)

#     dict_purchase_order = {
#         'Table_name': 'purchase_order',
#         'Headers':' hi',
#         'Data': list_purchase_order
#     }

#     with ('./ingestion-function/data/purchase_order.json', 'w') as file:
#         file.write(json.dumps(dict_purchase_order))


# # Payment_type data

# def get_payment_type():

#     list_payment_type = []
#     payments = con.run('SELECT * FROM payment_type')

#     for payment_type in payments:
#         payment_type[2] = payment_type[2].strftime('%Y-%m-%dT%H:%M:%S.%f')
#         payment_type[3] = payment_type[3].strftime('%Y-%m-%dT%H:%M:%S.%f')
#         list_payment_type.append(payment_type)

#     dict_payment_type = {
#         'Table_name': 'payment_type',
#         'Headers': get_headers(),
#         'Data': list_payment_type
#     }

#     with ('./ingestion-function/data/payment_type.json', 'w') as file:
#         file.write(json.dumps(dict_payment_type))


# #  Transaction data

# def get_transaction():

#     list_transaction = []
#     transactions = con.run('SELECT * FROM transaction')

#     for transaction in transactions:
#         transaction[4] = transaction[4].strftime('%Y-%m-%dT%H:%M:%S.%f')
#         transaction[5] = transaction[5].strftime('%Y-%m-%dT%H:%M:%S.%f')
#         list_transaction.append(transaction)

#     dict_transaction = {
#         'Table_name': 'transaction',
#         'Headers': get_headers(),
#         'Data': list_transaction
#     }

#     with ('./ingestion-function/data/transaction.json', 'w') as file:
#         file.write(json.dumps(dict_transaction))

# # get_counterparty()
# # get_currency()
# # get_departments()
# get_design()
# # get_staff()
# # get_sales_order()
# # get_address()
# # get_payment()
# # get_purchase_order()
# # get_payment_type()
# # get_transaction()
