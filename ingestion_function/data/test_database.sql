-- plan for test database if needed 

-- Drop, create & connect to database

-- ctrl shift L to put square brackets as round brackets

DROP DATABASE IF EXISTS totes_test_database;
CREATE DATABASE totes_test_database;
\c totes_test_database; 

-- Create all tables

CREATE TABLE counterparty (
    counterparty_id SERIAL PRIMARY KEY,
    counterparty_legal_name VARCHAR,
    legal_address_id INT,
    commercial_contact VARCHAR,
    delivery_contact VARCHAR,
    created_at TIMESTAMP,
    last_updated TIMESTAMP
    )

CREATE TABLE currency (
    currency_id SERIAL PRIMARY KEY,
    currency_code VARCHAR,
    created_at TIMESTAMP,
    last_updated TIMESTAMP
)

CREATE TABLE department (
    department_id SERIAL PRIMARY KEY,
    department_name VARCHAR,
    location VARCHAR,
    manager VARCHAR,
    created_at TIMESTAMP,
    last_updated TIMESTAMP    
)

CREATE TABLE design (
    design_id SERIAL PRIMARY KEY,
    created_at TIMESTAMP,
    last_updated TIMESTAMP,
    design_name VARCHAR,
    file_location VARCHAR,
    file_name VARCHAR
)

CREATE TABLE staff (
    staff_id SERIAL PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    department_id INT,
    email_address varchar,
    created_at TIMESTAMP,
    last_updated TIMESTAMP
)

CREATE TABLE sales_order (
    sales_order_id SERIAL PRIMARY KEY,
    created_at TIMESTAMP,
    last_updated TIMESTAMP,
    design_id INT,
    staff_id INT,
    counterparty_id INT,
    units_sold INT,
    unit_price NUMERIC,
    currency_id INT,
    agreed_delivery_date VARCHAR,
    agreed_payment_date VARCHAR,
    agreed_delivery_location_id INT
)

CREATE TABLE address (
    address_id SERIAL PRIMARY KEY,
    address_line_1 VARCHAR,
    address_line_2 VARCHAR,
    district VARCHAR,
    city VARCHAR,
    postal_code VARCHAR,
    country VARCHAR,
    phone VARCHAR,
    created_at TIMESTAMP,
    last_updated TIMESTAMP
)

CREATE TABLE payment (
    payment_id SERIAL PRIMARY KEY,
    created_at TIMESTAMP,
    last_updated TIMESTAMP,
    transaction_id INT,
    counterparty_id INT,
    payment_amount NUMERIC,
    currency_id INT,
    payment_type_id INT,
    paid BOOLEAN,
    payment_date VARCHAR,
    company_ac_number INT,
    counterparty_ac_number INT    
)

CREATE TABLE purchase_order (
    purchase_order_id INT,
    created_at TIMESTAMP,
    last_updated TIMESTAMP,
    staff_id INT,
    counterparty_id INT,
    item_code VARCHAR,
    item_quantity INT,
    item_unit_price NUMERIC,
    currency_id INT,
    agreed_delivery_date VARCHAR,
    agreed_delivery_date VARCHAR,
    agreed_delivery_location_id INT   
)

CREATE TABLE payment_type (
    payment_type_id INT,
    payment_type_name VARCHAR,
    created_at TIMESTAMP,
    last_updated TIMESTAMP
)

CREATE TABLE transaction (
    transaction_id INT,
    transaction_type VARCHAR,
    sales_order_id INT,
    purchase_order_id INT,
    created_at TIMESTAMP,
    last_updated TIMESTAMP
)

-- Insert data into tables

INSERT INTO counterparty(counterparty_legal_name, legal_address_id,commercial_contact, delivery_contact, created_at, last_updated)
VALUES
()


INSERT INTO currency(currency_id, currency_code, created_at last_updated)
VALUES
()

INSERT INTO department(department_id, department_name, location, manager, created_at, last_updated)
VALUES
()

INSERT INTO design(design_id, created_at, last_updated, design_name, file_location, file_name)
VALUES
()

INSERT INTO staff(staff_id,
    first_name,
    last_name,
    department_id,
    email_address,
    created_at,
    last_updated)
VALUES
()


INSERT INTO sales_order(sales_order_id,
    created_at,
    last_updated,
    design_id,
    staff_id,
    counterparty_id,
    units_sold,
    unit_price,
    currency_id,
    agreed_delivery_date,
    agreed_payment_date,
    agreed_delivery_location_id)
VALUES
()


INSERT INTO address(address_id,
    address_line_1, 
    address_line_2,
    district,
    city,
    postal_code,
    country,
    phone,
    created_at,
    last_updated)
VALUES
()

INSERT INTO payment(payment_id,
    created_at,
    last_updated,
    transaction_id,
    counterparty_id,
    payment_amount,
    currency_id,
    payment_type_id,
    paid,
    payment_date,
    company_ac_number,
    counterparty_ac_number)
VALUES
()

INSERT INTO purchase_order(purchase_order_id,
    created_at,
    last_updated,
    staff_id,
    counterparty_id,
    item_code,
    item_quantity,
    item_unit_price,
    currency_id,
    agreed_delivery_date,
    agreed_delivery_date ,
    agreed_delivery_location_id)
VALUES
()

INSERT INTO payment_type(payment_type_id,
    payment_type_name,
    created_at,
    last_updated)
VALUES
()

INSERT INTO transaction(transaction_id,
    transaction_type,
    sales_order_id,
    purchase_order_id,
    created_at,
    last_updated)
VALUES
()



