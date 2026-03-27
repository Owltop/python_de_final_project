import pandas as pd
df = pd.read_parquet("data/chunk_1020001_1030000.parquet")

print("Уникальных order_id:", df["order_id"].nunique())
print("Строк всего:", len(df))
print("\nПримеры order_cancellation_reason:")
print(df["order_cancellation_reason"].value_counts(dropna=False).head(10))
print("\nПримеры payment_type:")
print(df["payment_type"].value_counts(dropna=False))
print("\nСколько файлов parquet у тебя в папке data?")
