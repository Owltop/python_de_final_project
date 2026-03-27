#!/usr/bin/env python3

from sqlalchemy import create_engine, text

engine = create_engine('postgresql://postgres:postgres@localhost:5432/delivery_db')
conn = engine.connect()

conn.execute(text("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY,
        user_phone VARCHAR(50)
    )
"""))

conn.execute(text("""
    CREATE TABLE IF NOT EXISTS driver (
        driver_id INT PRIMARY KEY,
        driver_phone VARCHAR(50)
    )
"""))

conn.execute(text("""
    CREATE TABLE IF NOT EXISTS store (
        store_id INT PRIMARY KEY,
        store_address TEXT
    )
"""))

conn.execute(text("""
    CREATE TABLE IF NOT EXISTS item (
        item_id INT PRIMARY KEY,
        item_title VARCHAR(255),
        item_category VARCHAR(100)
    )
"""))

conn.execute(text("""
    CREATE TABLE IF NOT EXISTS orders (
        order_id INT PRIMARY KEY,
        user_id INT,
        store_id INT,
        driver_id INT,
        created_at TIMESTAMP,
        address_text TEXT,
        paid_at TIMESTAMP,
        delivery_started_at TIMESTAMP,
        delivered_at TIMESTAMP,
        canceled_at TIMESTAMP,
        payment_type VARCHAR(50),
        order_discount DECIMAL(5,2),
        order_cancellation_reason TEXT,
        delivery_cost DECIMAL(10,2))
"""))

conn.execute(text("""
    CREATE TABLE IF NOT EXISTS order_item (
        order_id INT,
        item_id INT,
        item_quantity INT,
        item_price DECIMAL(10,2),
        item_discount DECIMAL(5,2),
        item_canceled_quantity INT,
        item_replaced_id INT,
        PRIMARY KEY (order_id, item_id)
    )
"""))

conn.commit()
conn.close()

