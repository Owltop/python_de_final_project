CREATE TABLE IF NOT EXISTS users (
    user_id    BIGINT,
    user_phone TEXT
);

CREATE TABLE IF NOT EXISTS drivers (
    driver_id    BIGINT,
    driver_phone TEXT
);

CREATE TABLE IF NOT EXISTS store (
    store_id      BIGINT,
    store_address TEXT
);

CREATE TABLE IF NOT EXISTS item (
    item_id       BIGINT,
    item_title    TEXT,
    item_category TEXT
);

CREATE TABLE IF NOT EXISTS orders (
    order_id                  BIGINT,
    user_id                   BIGINT,
    store_id                  BIGINT,
    driver_id                 BIGINT,
    created_at                TIMESTAMP,
    address_text              TEXT,
    paid_at                   TIMESTAMP,
    delivery_started_at       TIMESTAMP,
    delivered_at              TIMESTAMP,
    canceled_at               TIMESTAMP,
    payment_type              TEXT,
    order_discount            NUMERIC(5,2),
    order_cancellation_reason TEXT,
    delivery_cost             NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS order_item (
    order_id               BIGINT,
    item_id                BIGINT,
    item_quantity          BIGINT,
    item_price             NUMERIC(10,2),
    item_discount          NUMERIC(5,2),
    item_canceled_quantity BIGINT,
    item_replaced_id       BIGINT
);

CREATE TABLE IF NOT EXISTS mart_orders (
    year                            INTEGER,
    month                           INTEGER,
    day                             INTEGER,
    city                            TEXT,
    store_id                        BIGINT,
    turnover                        NUMERIC(14,2),
    revenue                         NUMERIC(14,2),
    profit                          NUMERIC(14,2),
    orders_created                  BIGINT,
    orders_delivered                BIGINT,
    orders_canceled                 BIGINT,
    orders_canceled_after_delivery  BIGINT,
    orders_canceled_service_error   BIGINT,
    unique_customers                BIGINT,
    avg_check                       NUMERIC(14,2),
    orders_per_customer             NUMERIC(10,2),
    revenue_per_customer            NUMERIC(14,2),
    active_couriers                 BIGINT,
    courier_changes                 INTEGER
);

CREATE TABLE IF NOT EXISTS mart_items (
    year                        INTEGER,
    month                       INTEGER,
    day                         INTEGER,
    city                        TEXT,
    store_id                    BIGINT,
    item_id                     BIGINT,
    item_title                  TEXT,
    item_category               TEXT,
    item_turnover               NUMERIC(14,2),
    units_ordered               BIGINT,
    units_canceled              BIGINT,
    orders_with_item            BIGINT,
    orders_with_item_canceled   BIGINT
);
