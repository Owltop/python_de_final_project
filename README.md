## Запуск

### ARM (Apple Silicon — M1/M2/M3/M4)

```bash
docker-compose up --build
```

### Все остальные архитектуры (Intel/AMD x86_64)

```bash
git checkout test_branch
docker-compose up --build
```

> Причина: образы Docker для Airflow и Java имеют разные бинарники
> под ARM64 и AMD64 архитектуры, поэтому путь к JVM отличается
> (`java-17-openjdk-arm64` vs `java-17-openjdk-amd64`).

---

### Шаг 1 — Запуск пайплайна в Airflow

1. Открой в браузере http://localhost:8080
2. Найди DAG `load_normalized_data`
3. Включи его (переключатель слева от названия)
4. Нажми кнопку ▶ (Trigger DAG) и дождись статуса **success**
5. Найди DAG `build_marts`
6. Включи его и нажми кнопку ▶
7. Дождись статуса **success**

---

### Шаг 2 — Просмотр витрин в pgAdmin

1. Открой в браузере http://localhost:5050
2. Подключись к серверу — нажми **Add New Server** и заполни параметры:

| Параметр | Значение |
|----------|----------|
| Host | postgres |
| Port | 5432 |
| Database | delivery_db |
| Username | postgres |
| Password | postgres |

3. В левом меню раскрой:
```
Servers
└── postgres
    └── Databases
        └── delivery_db
            └── Schemas
                └── public
                    └── Tables
                        ├── mart_items   ← витрина товаров
                        └── mart_orders  ← витрина заказов
```
4. Нажми правой кнопкой на таблицу → **View/Edit Data → All Rows**
