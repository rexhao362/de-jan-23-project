# Data flow
The document describes output data structure of each module in the workflow

## Ingest
add detailed specification of the output data here

## Transform
Outputs:
Seven .parquet files, each representing the following tables:

fact_sales_order(sales_record_id, sales_order_id, created_date, created_time, last_updated_date, last_updated_time, slaes_staff_id, counterparty_id, units_sold, unit_price, currency_id, design_id, agreed_payment_date, agreed_delivery_date, agreed_delivery_location_id)
dim_staff(staff_id, first_name, last_name, department_name, location, email_address)
dim_location(location_id, address_line_1, address_line_2, district, city, postal_code, country, phone)
dim_design(design_id, design_name, file_location, file_name)
dim_date(date_id, year, month, day, day_of_week, day_name, month_name, quarter)
dim_currency(currency_id, currency_code, currency_name)
dim_counterparty(counterparty_id, counterparty_legal_name, counterparty_legal_address_line_1, counterparty_legal_address_line_2, counterparty_legal_district, counterparty_legal_city, counterparty_legal_postal_code, counterparty_legal_country, counterparty_legal_phone_number)

Metadata yet to be determined, potentially to be stored in the filename.

## Load
add detailed specification of the output data here
