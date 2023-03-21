import requests
import pg8000.native
import os
import json

# # host = 'nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com'
# # port = 5432
# # user = 'project_user_1'
# # password = 'UmaC43m32Zi6RW'
# # database = 'totesys'
# # schema = 'public'

# DB connection
con = pg8000.native.Connection('project_user_1', host='nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com', database='totesys', port=5432, password='UmaC43m32Zi6RW')

# Create data dir
if not os.path.exists('ingestion-function/data'):
    os.makedirs('ingestion-function/data')

# Counterparty data


# Currency data


# Departments data
departments = con.run('SELECT * FROM department')
print(departments)

new_list = []

for department in departments:
    dict = {'department_id': department[0],
    'department_name': department[1],
    'location': department[2],
    'manager': department[3],
    'created_at': department[4].strftime('%Y-%m-%dT%H:%M:%-S'),
    'last_updated': department[5].strftime('%Y-%m-%dT%H:%M:%-S')
    }
    new_list.append(dict)

print(new_list)

with open('./ingestion-function/data/departments.json', 'w') as file:
    file.write(json.dumps(new_list))

# Design data

# Staff data

# Sales_order data

# Address data

# Payment data

# Purchase_order data

# Payment_type data

# Transaction data
