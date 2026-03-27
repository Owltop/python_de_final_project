-- ============================================================
-- НОРМАЛИЗАЦИЯ ДО 3NF
--
-- Исходная таблица нарушала:
--   1NF: повторяющиеся группы товаров внутри заказа
--   2NF: item_title/item_category зависят от item_id, а не от (order_id, item_id)
--   3NF: store_address зависит от store_id, driver_phone от driver_id и т.д.
--
-- Декомпозиция:
--   store      — справочник магазинов (store_id → store_address)
--   item       — справочник товаров   (item_id  → title, category)
--   orders     — заказы               (order_id → все атрибуты заказа)
--   order_item — позиции заказа       (order_id + item_id → количество, цена и т.д.)
-- ============================================================


-- ------------------------------------------------------------
-- Справочник магазинов
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS store (
    store_id        BIGINT PRIMARY KEY,
    store_address   TEXT
);

-- ------------------------------------------------------------
-- Справочник товаров
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS item (
    item_id         BIGINT PRIMARY KEY,
    item_title      TEXT,
    item_category   TEXT
);

-- ------------------------------------------------------------
-- Заказы
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS orders (
    order_id                    BIGINT PRIMARY KEY,
    user_id                     BIGINT,
    store_id                    BIGINT REFERENCES store(store_id),
    driver_id                   BIGINT,
    created_at                  TIMESTAMP,
    address_text                TEXT,
    paid_at                     TIMESTAMP,
    delivery_started_at         TIMESTAMP,
    delivered_at                TIMESTAMP,
    canceled_at                 TIMESTAMP,
    payment_type                TEXT,
    order_discount              NUMERIC(5,2),
    order_cancellation_reason   TEXT,
    delivery_cost               NUMERIC(10,2)
);

-- ------------------------------------------------------------
-- Позиции заказа
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS order_item (
    id                      SERIAL PRIMARY KEY,
    order_id                BIGINT REFERENCES orders(order_id),
    item_id                 BIGINT REFERENCES item(item_id),
    item_quantity           BIGINT,
    item_price              NUMERIC(10,2),
    item_discount           NUMERIC(5,2),
    item_canceled_quantity  BIGINT,
    item_replaced_id        BIGINT
);

-- ------------------------------------------------------------
-- Витрина заказов
-- ------------------------------------------------------------
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

-- ------------------------------------------------------------
-- Витрина товаров
-- ------------------------------------------------------------
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
