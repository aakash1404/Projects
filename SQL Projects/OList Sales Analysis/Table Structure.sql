DROP DATABASE IF EXISTS olist;

CREATE DATABASE olist;

USE olist;

DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS geolocation;
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS payments;
DROP TABLE IF EXISTS sellers;

CREATE TABLE customers(customer_id varchar(32),
                       customer_unique_id varchar(32),
                       customer_zip_code_prefix int,
                       customer_city varchar(32),
                       customer_state char(2));
                       
CREATE TABLE orders (order_id VARCHAR(32),
                     customer_id VARCHAR(32),
                     order_status VARCHAR(11),
                     order_purchase_timestamp Timestamp,
                     order_approved_at Timestamp,
                     order_delivered_carrier_date Timestamp,
                     order_delivered_customer_date Timestamp,
                     order_estimated_delivery_date Timestamp);
                     
CREATE TABLE geolocation (geolocation_zip_code_prefix INT,
                          geolocation_lat FLOAT,
                          geolocation_lng FLOAT,
                          geolocation_city VARCHAR(38),
                          geolocation_state VARCHAR(2));
                          
CREATE TABLE order_items (order_id VARCHAR(32),
                         order_item_id INT,
                         product_id VARCHAR(32),
                         seller_id VARCHAR(32),
                         shipping_limit_date TIMESTAMP,
                         price FLOAT,
                         freight_value FLOAT);
		
CREATE TABLE payments (order_id VARCHAR(32),
                       payment_sequential INT,
                       payment_type VARCHAR(11),
                       payment_installments INT,
                       payment_value FLOAT);
                       
CREATE TABLE sellers (seller_id VARCHAR(32),
                      seller_zip_code_prefix INT,
                      seller_city VARCHAR(40),
                      seller_state VARCHAR(2));
                      