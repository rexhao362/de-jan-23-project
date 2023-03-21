import os
import json
from connection import con


# Create data dir
if not os.path.exists('ingestion-function/data'):
    os.makedirs('ingestion-function/data')

# Counterparty data


def get_counterparty():
    list_counterparty = []
    counterparties = con.run('SELECT * FROM counterparty')

    for counterparty in counterparties:
        counterparty[5] = counterparty[5].strftime('%Y-%m-%dT%H:%M:%S.%f')
        counterparty[6] = counterparty[6].strftime('%Y-%m-%dT%H:%M:%S.%f')
        list_counterparty.append(counterparty)

    dict_counterparty = {
        'Table_name': 'counterparty',
        'Headers':,
        'Data': list_counterparty
    }

    with open('./ingestion-function/data/counterparty.json', 'w') as file:
        file.write(json.dumps(dict_counterparty))


get_counterparty()

# Currency data


def get_currency():
    headers = con.run("select * from currency where false")
    print(headers)

    list_currency = []
    currencies = con.run('SELECT * FROM currency')

    for currency in currencies:
        currency[2] = currency[2].strftime('%Y-%m-%dT%H:%M:%S.%f')
        currency[3] = currency[3].strftime('%Y-%m-%dT%H:%M:%S.%f')
        list_currency.append(currency)

    dict_currency = {
        'Table_name': 'currency',
        'Headers':,
        'Data': list_currency
    }

    # 'created_at': currency[2].strftime('%Y-%m-%dT%H:%M:%S.%f'),
    # 'last_updated': currency[3].strftime('%Y-%m-%dT%H:%M:%S.%f')

    with open('./ingestion-function/data/currency.json', 'w') as file:
        file.write(json.dumps(dict_currency))


get_currency()

# Departments data


def get_departments():
    list_department = []
    departments = con.run('SELECT * FROM department')

    for department in departments:
        department[4] = department[4].strftime('%Y-%m-%dT%H:%M:%S.%f')
        department[5] = department[5].strftime('%Y-%m-%dT%H:%M:%S.%f')
        list_department.append(department)

    dict_department = {
        'Table_name': 'department',
        'Headers':,
        'Data': list_department
    }

    with open('./ingestion-function/data/departments.json', 'w') as file:
        file.write(json.dumps(dict_department))


get_departments()

#  Design data


def get_design():
    list_design = []
    designs = con.run('SELECT * FROM design')

    for design in designs:
        design[1] = design[1].strftime('%Y-%m-%dT%H:%M:%S.%f')
        design[2] = design[2].strftime('%Y-%m-%dT%H:%M:%S.%f')
        list_design.append(design)

    dict_design = {
        'Table_name': 'design',
        'Headers':,
        'Data': list_design
    }

    with open('./ingestion-function/data/design.json', 'w') as file:
        file.write(json.dumps(dict_design))


# get_design()

#  Staff data
def get_staff():
    list_staff = []
    staff = con.run('SELECT * FROM staff')

    for person in staff:
        person[1] = person[1].strftime('%Y-%m-%dT%H:%M:%S.%f')
        person[2] = person[2].strftime('%Y-%m-%dT%H:%M:%S.%f')
        list_staff.append(person)

    dict_staff = {
        'Table_name': 'staff',
        'Headers':,
        'Data': list_staff
    }

    with open('./ingestion-function/data/staff.json', 'w') as file:
        file.write(json.dumps(dict_staff))

# get_staff()

#  Sales_order data
    list_sales_order = []
    orders = con.run('SELECT * FROM sales_order')

    for sales_order in orders:
        sales_order[1] = sales_order[1].strftime('%Y-%m-%dT%H:%M:%S.%f')
        sales_order[2] = sales_order[2].strftime('%Y-%m-%dT%H:%M:%S.%f')
        list_sales_order.append(sales_order)

    dict_sales_order = {
        'Table_name': 'sales_order',
        'Headers':,
        'Data': list_sales_order
    }

    with ('./ingestion-function/data/sales_order.json', 'w') as file:
        file.write(json.dumps(dict_sales_order))

# Address data
    list_address = []
    addresses = con.run('SELECT * FROM address')

    for address in addresses:
        address[8] = address[8].strftime('%Y-%m-%dT%H:%M:%S.%f')
        address[9] = address[9].strftime('%Y-%m-%dT%H:%M:%S.%f')
        list_address.append(address)

    dict_address = {
        'Table_name': 'address',
        'Headers':,
        'Data': list_address
    }

    with ('./ingestion-function/data/address.json', 'w') as file:
        file.write(json.dumps(dict_address))

# Payment data
    list_payment = []
    payments = con.run('SELECT * FROM payment')

    for payment in payments:
        payment[1] = payment[1].strftime('%Y-%m-%dT%H:%M:%S.%f')
        payment[2] = payment[2].strftime('%Y-%m-%dT%H:%M:%S.%f')
        list_payment.append(payment)

    dict_payment = {
        'Table_name': 'payment',
        'Headers':,
        'Data': list_payment
    }

    with ('./ingestion-function/data/payment.json', 'w') as file:
        file.write(json.dumps(dict_payment))

#  Purchase_order data
    list_purchase_order = []
    purchases = con.run('SELECT * FROM purchase_order')

    for purchase_order in purchases:
        purchase_order[1] = purchase_order[1].strftime('%Y-%m-%dT%H:%M:%S.%f')
        purchase_order[2] = purchase_order[2].strftime('%Y-%m-%dT%H:%M:%S.%f')
        list_purchase_order.append(purchase_order)

    dict_purchase_order = {
        'Table_name': 'purchase_order',
        'Headers':,
        'Data': list_purchase_order
    }

    with ('./ingestion-function/data/purchase_order.json', 'w') as file:
        file.write(json.dumps(dict_purchase_order))

# Payment_type data
    list_payment_type = []
    payments = con.run('SELECT * FROM payment_type')

    for payment_type in payments:
        payment_type[2] = payment_type[2].strftime('%Y-%m-%dT%H:%M:%S.%f')
        payment_type[3] = payment_type[3].strftime('%Y-%m-%dT%H:%M:%S.%f')
        list_payment_type.append(payment_type)

    dict_payment_type = {
        'Table_name': 'payment_type',
        'Headers':,
        'Data': list_payment_type
    }

    with ('./ingestion-function/data/payment_type.json', 'w') as file:
        file.write(json.dumps(dict_payment_type))

#  Transaction data
    list_transaction = []
    transactions = con.run('SELECT * FROM transaction')

    for transaction in transactions:
        transaction[4] = transaction[4].strftime('%Y-%m-%dT%H:%M:%S.%f')
        transaction[5] = transaction[5].strftime('%Y-%m-%dT%H:%M:%S.%f')
        list_transaction.append(transaction)

    dict_transaction = {
        'Table_name': 'transaction',
        'Headers':,
        'Data': list_transaction
    }

    with ('./ingestion-function/data/transaction.json', 'w') as file:
        file.write(json.dumps(dict_transaction))
