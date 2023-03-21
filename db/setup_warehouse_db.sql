DROP DATABASE IF EXISTS totesys_warehouse_db;
CREATE DATABASE totesys_warehouse_db;

\c totesys_warehouse_db

CREATE TABLE "fact_sales_order" (
  "sales_record_id" SERIAL PRIMARY KEY,
  "sales_order_id" int NOT NULL,
  "created_date" date NOT NULL,
  "created_time" time NOT NULL,
  "last_updated_date" date NOT NULL,
  "last_updated_time" time NOT NULL,
  "sales_staff_id" int NOT NULL,
  "counterparty_id" int NOT NULL,
  "units_sold" int NOT NULL,
  "unit_price" numeric(10, 2) NOT NULL,
  "currency_id" int NOT NULL,
  "design_id" int NOT NULL,
  "agreed_payment_date" date NOT NULL,
  "agreed_delivery_date" date NOT NULL,
  "agreed_delivery_location_id" int NOT NULL
);

CREATE TABLE "dim_date" (
  "date_id" date PRIMARY KEY NOT NULL,
  "year" int NOT NULL,
  "month" int NOT NULL,
  "day" int NOT NULL,
  "day_of_week" int NOT NULL,
  "day_name" varchar NOT NULL,
  "month_name" varchar NOT NULL,
  "quarter" int NOT NULL
);

CREATE TABLE "dim_staff" (
  "staff_id" int PRIMARY KEY NOT NULL,
  "first_name" varchar NOT NULL,
  "last_name" varchar NOT NULL,
  "department_name" varchar NOT NULL,
  "location" varchar NOT NULL,
  "email_address" varchar NOT NULL
);

CREATE TABLE "dim_location" (
  "location_id" int PRIMARY KEY NOT NULL,
  "address_line_1" varchar NOT NULL,
  "address_line_2" varchar,
  "district" varchar,
  "city" varchar NOT NULL,
  "postal_code" varchar NOT NULL,
  "country" varchar NOT NULL,
  "phone" varchar NOT NULL
);

CREATE TABLE "dim_currency" (
  "currency_id" int PRIMARY KEY NOT NULL,
  "currency_code" varchar NOT NULL,
  "currency_name" varchar NOT NULL
);

CREATE TABLE "dim_design" (
  "design_id" int PRIMARY KEY NOT NULL,
  "design_name" varchar NOT NULL,
  "file_location" varchar NOT NULL,
  "file_name" varchar NOT NULL
);

CREATE TABLE "dim_counterparty" (
  "counterparty_id" int PRIMARY KEY NOT NULL,
  "counterparty_legal_name" varchar NOT NULL,
  "counterparty_legal_address_line_1" varchar NOT NULL,
  "counterparty_legal_address_line_2" varchar,
  "counterparty_legal_district" varchar,
  "counterparty_legal_city" varchar NOT NULL,
  "counterparty_legal_postal_code" varchar NOT NULL,
  "counterparty_legal_country" varchar NOT NULL,
  "counterparty_legal_phone_number" varchar NOT NULL
);

CREATE TABLE "fact_purchase_order" (
  "purchase_record_id" SERIAL PRIMARY KEY,
  "purchase_order_id" INT NOT NULL,
  "created_date" date NOT NULL,
  "created_time" time NOT NULL,
  "last_updated_date" date NOT NULL,
  "last_updated_time" time NOT NULL,
  "staff_id" int NOT NULL,
  "counterparty_id" int NOT NULL,
  "item_code" varchar NOT NULL,
  "item_quantity" int NOT NULL,
  "item_unit_price" numeric NOT NULL,
  "currency_id" int NOT NULL,
  "agreed_delivery_date" date NOT NULL,
  "agreed_payment_date" date NOT NULL,
  "agreed_delivery_location_id" int NOT NULL
);

CREATE TABLE "dim_payment_type" (
  "payment_type_id" INT PRIMARY KEY NOT NULL,
  "payment_type_name" varchar NOT NULL
);

CREATE TABLE "fact_payment" (
  "payment_record_id" SERIAL PRIMARY KEY,
  "payment_id" int NOT NULL,
  "created_date" date NOT NULL,
  "created_time" time NOT NULL,
  "last_updated_date" date NOT NULL,
  "last_updated" time NOT NULL,
  "transaction_id" int NOT NULL,
  "counterparty_id" int NOT NULL,
  "payment_amount" numeric NOT NULL,
  "currency_id" int NOT NULL,
  "payment_type_id" int NOT NULL,
  "paid" boolean NOT NULL,
  "payment_date" date NOT NULL
);

CREATE TABLE "dim_transaction" (
  "transaction_id" int PRIMARY KEY NOT NULL,
  "transaction_type" varchar NOT NULL,
  "sales_order_id" int,
  "purchase_order_id" int
);

ALTER TABLE "fact_payment" ADD FOREIGN KEY ("created_date") REFERENCES "dim_date" ("date_id");

ALTER TABLE "fact_payment" ADD FOREIGN KEY ("last_updated_date") REFERENCES "dim_date" ("date_id");

ALTER TABLE "fact_payment" ADD FOREIGN KEY ("transaction_id") REFERENCES "dim_transaction" ("transaction_id");

ALTER TABLE "fact_payment" ADD FOREIGN KEY ("counterparty_id") REFERENCES "dim_counterparty" ("counterparty_id");

ALTER TABLE "fact_payment" ADD FOREIGN KEY ("currency_id") REFERENCES "dim_currency" ("currency_id");

ALTER TABLE "fact_payment" ADD FOREIGN KEY ("payment_type_id") REFERENCES "dim_payment_type" ("payment_type_id");

ALTER TABLE "fact_payment" ADD FOREIGN KEY ("payment_date") REFERENCES "dim_date" ("date_id");

ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("created_date") REFERENCES "dim_date" ("date_id");

ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("last_updated_date") REFERENCES "dim_date" ("date_id");

ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("sales_staff_id") REFERENCES "dim_staff" ("staff_id");

ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("counterparty_id") REFERENCES "dim_counterparty" ("counterparty_id");

ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("currency_id") REFERENCES "dim_currency" ("currency_id");

ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("design_id") REFERENCES "dim_design" ("design_id");

ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("agreed_payment_date") REFERENCES "dim_date" ("date_id");

ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("agreed_delivery_date") REFERENCES "dim_date" ("date_id");

ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("agreed_delivery_location_id") REFERENCES "dim_location" ("location_id");

ALTER TABLE "fact_purchase_order" ADD FOREIGN KEY ("created_date") REFERENCES "dim_date" ("date_id");

ALTER TABLE "fact_purchase_order" ADD FOREIGN KEY ("last_updated_date") REFERENCES "dim_date" ("date_id");

ALTER TABLE "fact_purchase_order" ADD FOREIGN KEY ("staff_id") REFERENCES "dim_staff" ("staff_id");

ALTER TABLE "fact_purchase_order" ADD FOREIGN KEY ("counterparty_id") REFERENCES "dim_counterparty" ("counterparty_id");

ALTER TABLE "fact_purchase_order" ADD FOREIGN KEY ("currency_id") REFERENCES "dim_currency" ("currency_id");

ALTER TABLE "fact_purchase_order" ADD FOREIGN KEY ("agreed_delivery_date") REFERENCES "dim_date" ("date_id");

ALTER TABLE "fact_purchase_order" ADD FOREIGN KEY ("agreed_payment_date") REFERENCES "dim_date" ("date_id");

ALTER TABLE "fact_purchase_order" ADD FOREIGN KEY ("agreed_delivery_location_id") REFERENCES "dim_location" ("location_id");
