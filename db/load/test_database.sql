-- plan for test database if needed 

-- Drop, create & connect to database

-- ctrl shift L to put square brackets as round brackets

DROP DATABASE IF EXISTS totes_test_database;
CREATE DATABASE totes_test_database;
\c totes_test_database; 

-- Create all tables
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;


CREATE TABLE public.address (
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
);

CREATE TABLE public.counterparty (
    counterparty_id SERIAL PRIMARY KEY,
    counterparty_legal_name VARCHAR,
    legal_address_id INT REFERENCES address(address_id),
    commercial_contact VARCHAR,
    delivery_contact VARCHAR,
    created_at TIMESTAMP,
    last_updated TIMESTAMP
);

CREATE TABLE public.currency (
    currency_id SERIAL PRIMARY KEY,
    currency_code VARCHAR,
    created_at TIMESTAMP,
    last_updated TIMESTAMP
);

CREATE TABLE public.department (
    department_id SERIAL PRIMARY KEY,
    department_name VARCHAR,
    location VARCHAR,
    manager VARCHAR,
    created_at TIMESTAMP,
    last_updated TIMESTAMP    
);

CREATE TABLE public.design (
    design_id SERIAL PRIMARY KEY,
    created_at TIMESTAMP,
    last_updated TIMESTAMP,
    design_name VARCHAR,
    file_location VARCHAR,
    file_name VARCHAR
);

CREATE TABLE public.staff (
    staff_id SERIAL PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    department_id INT,
    email_address varchar,
    created_at TIMESTAMP,
    last_updated TIMESTAMP
);

CREATE TABLE public.sales_order (
    sales_order_id SERIAL PRIMARY KEY,
    created_at TIMESTAMP,
    last_updated TIMESTAMP,
    design_id INT REFERENCES design(design_id),
    staff_id INT REFERENCES staff(staff_id),
    counterparty_id INT REFERENCES counterparty(counterparty_id),
    units_sold INT,
    unit_price NUMERIC(10, 2),
    currency_id INT,
    agreed_delivery_date VARCHAR,
    agreed_payment_date VARCHAR,
    agreed_delivery_location_id INT
);

CREATE TABLE public.purchase_order (
    purchase_order_id SERIAL PRIMARY KEY,
    created_at TIMESTAMP,
    last_updated TIMESTAMP,
    staff_id INT REFERENCES staff(staff_id),
    counterparty_id INT REFERENCES counterparty(counterparty_id),
    item_code VARCHAR,
    item_quantity INT,
    item_unit_price NUMERIC,
    currency_id INT REFERENCES currency(currency_id),
    agreed_delivery_date VARCHAR,
    agreed_payment_date VARCHAR,
    agreed_delivery_location_id INT   
);

CREATE TABLE public.payment_type (
    payment_type_id SERIAL PRIMARY KEY,
    payment_type_name VARCHAR,
    created_at TIMESTAMP,
    last_updated TIMESTAMP
);

CREATE TABLE public.transaction (
    transaction_id SERIAL PRIMARY KEY,
    transaction_type VARCHAR,
    sales_order_id INT REFERENCES sales_order(sales_order_id),
    purchase_order_id INT REFERENCES purchase_order(purchase_order_id),
    created_at TIMESTAMP,
    last_updated TIMESTAMP
);

CREATE TABLE public.payment (
    payment_id SERIAL PRIMARY KEY,
    created_at TIMESTAMP,
    last_updated TIMESTAMP,
    transaction_id INT REFERENCES transaction(transaction_id),
    counterparty_id INT REFERENCES counterparty(counterparty_id),
    payment_amount NUMERIC,
    currency_id INT REFERENCES currency(currency_id),
    payment_type_id INT REFERENCES payment_type(payment_type_id),
    paid BOOLEAN,
    payment_date VARCHAR,
    company_ac_number INT,
    counterparty_ac_number INT    
);

-- Insert data into tables
INSERT INTO address(
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
('6826 Herzog Via', null, 'Avon', 'New Patienceburgh', '28441', 'Turkey', '1803 637401', TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss')), ('179 Alexie Cliffs', null, null, 'Aliso Viejo', '99305-7380', 'San Marino', '9621 880720', TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss')), ('148 Sincere Fort', null, null, 'Lake Charles', '89360', 'Samoa', '0730 783349', TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss')), ('6102 Rogahn Skyway', null, 'Bedfordshire', 'Olsonside', '47518', 'Republic of Korea', '1239 706295', TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss')), ('34177 Upton Track', null, null, 'Fort Shadburgh', '55993-8850', 'Bosnia and Herzegovina', '0081 009772', TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'));

INSERT INTO counterparty(counterparty_legal_name, legal_address_id, commercial_contact, delivery_contact, created_at, last_updated)
VALUES
('Fahey and Sons', 1, 'Micheal Toy', 'Mrs. Lucy Runolfsdottir', TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss')), ('Leannon, Predovic and Morar', 2, 'Melba Sanford', 'Jean Hane III', TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss')), ('Armstrong Inc', 2, 'Jane Wiza', 'Myra Kovacek', TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss')), ('Kohler Inc', 4, 'Taylor Haag', 'Alfredo Cassin II', TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss')), ('Frami, Yundt and Macejkovic', 4, 'Homer Mitchell', 'Ivan Balistreri', TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'));

INSERT INTO currency(currency_code, created_at, last_updated)
VALUES
('GBP', TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss')), ('USD', TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss')), ('EUR', TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'));

INSERT INTO department(department_name, location, manager, created_at, last_updated)
VALUES
('Sales', 'Manchester', 'Richard Roma', TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss')),('Purchasing', 'Manchester', 'Naomi Lapaglia', TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss')),('Production', 'Leeds', 'Chester Ming', TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss')),('Dispatch', 'Leds', 'Mark Hanna', TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss')),('Finance', 'Manchester', 'Jordan Belfort', TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'));

INSERT INTO payment_type(
    payment_type_name,
    created_at,
    last_updated)
VALUES
('SALES_RECEIPT', TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss')), ('SALES_REFUND', TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss')), ('PURCHASE_PAYMENT', TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss')),('PURCHASE_REFUND', TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'));

INSERT INTO design(created_at, design_name, file_location, file_name, last_updated)
VALUES
(TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), 'Wooden', '/usr', 'wooden-20220717-npgz.json', TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss')),(TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), 'Bronze', '/private', 'bronze-20221024-4dds.json', TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss')),(TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), 'Granite', '/private/var', 'granite-20220205-3vfw.json', TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss')),(TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), 'Granite', '/private/var', 'granite-20210406-uwqg.json', TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss')),(TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), 'Frozen', '/Library', 'frozen-20201124-fsdu.json', TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'));


INSERT INTO staff(
    first_name,
    last_name,
    department_id,
    email_address,
    created_at,
    last_updated)
VALUES
('Jeremie', 'Franey', 2, 'jeremie.franey@terrifictotes.com', TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss')),('Deron', 'Beier', 5, 'deron.beier@terrifictotes.com', TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss')),('Jeanette', 'Erdman', 3, 'jeanette.erdman@terrifictotes.com', TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss')),('Ana', 'Glover', 3, 'ana.glover@terrifictotes.com', TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss')),('Magdalena', 'Zieme', 1, 'magdalena.zieme@terrifictotes.com', TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'));


INSERT INTO sales_order(
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
(TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), 1, 3, 5, 84754, 2.43, 3, '2022-11-10', '2022-11-03', 4),(TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), 3, 3, 4, 42972, 3.94, 2, '2022-11-07', '2022-11-08', 8), (TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), 4, 5, 4, 65839, 2.91, 3, '2022-11-06', '2022-11-07', 19), (TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), 4, 2, 1, 32069, 3.89, 2, '2022-11-05', '2022-11-07', 15), (TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), 1, 1, 4, 49659, 2.41, 3, '2022-11-05', '2022-11-08', 25);

INSERT INTO purchase_order(
    created_at,
    last_updated,
    staff_id,
    counterparty_id,
    item_code,
    item_quantity,
    item_unit_price,
    currency_id,
    agreed_delivery_date,
    agreed_payment_date ,
    agreed_delivery_location_id)
VALUES
(TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), 1, 1, 'ZDOI5EA', 371, 361.39, 2, '2022-11-09', '2022-11-07', 6), (TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), 2, 5, 'QLZLEXR', 286, 199.04, 2, '2022-11-04', '2022-11-07', 8), (TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), 2, 2, 'AN3D85L', 839, 658.58, 2, '2022-11-05', '2022-11-04', 16), (TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), 4, 2, 'I9MET53', 316, 803.82, 3, '2022-11-10', '2022-11-05', 2), (TO_TIMESTAMP('2023-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2023-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), 5, 3, 'QKQQ9IS', 597, 714.89, 2, '2022-12-03', '2022-12-03', 11);

INSERT INTO transaction(
    transaction_type,
    sales_order_id,
    purchase_order_id,
    created_at,
    last_updated)
VALUES
('PURCHASE', null, 3, TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss')), ('PURCHASE', null, 3, TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss')), ('SALE', 1, null, TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss')), ('PURCHASE', null, 1, TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss')), ('PURCHASE', null, 4, TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'));

INSERT INTO payment(
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
(TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), 2, 5, 552548.62, 2, 3, false, '2022-11-04', 67305075, 31622269), (TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), 3, 2, 205952.22, 3, 1, false, '2022-11-03', 81718079, 47839086), (TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), 5, 1, 57067.2, 2, 3, false, '2022-11-06', 66213052, 91659548), (TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), 4, 2, 254007.12, 3, 3, false, '2022-11-05', 32948439, 90135525), (TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), TO_TIMESTAMP('2022-11-03 14:20:49', 'yyyy-mm-dd hh24:mi:ss'), 5, 1, 250459.52, 2, 1, false, '2022-11-05', 34445327, 71673373);




