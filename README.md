### Как запустить?
На маке достаточно `docker-compose up --build`
---


Исходные данные нарушали 1NF (повторяющиеся группы товаров), 2NF (item_title зависит от item_id, а не от составного ключа), 3NF (store_address зависит от store_id, user_phone от user_id, driver_phone от driver_id). Данные декомпозированы до 3NF: справочники вынесены в отдельные таблицы, позиции заказа отделены от заказов

далее необходимо открыть в баузере http://localhost:8080

нажать включить load_normalized_data и нажать запуск dag

после того, как будет success, тоже самое проделать со вторым dag под названием build_marts

Далее перейти в http://localhost:5050 и открыть сервер postgres c праметрами:

Host: postgres
Port: 5432
Database: delivery_db
Username: postgres
Password: postgres

Далее откройте databases -> delivery_db -> schemas -> tables -> открываете mart_items и mart_orders
