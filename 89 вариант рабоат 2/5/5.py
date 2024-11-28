import pandas as pd
import numpy as np
import json
import pickle
import msgpack

from AppData.Local.Programs.Python.Python310.Lib import os

# Генерация искусственного набора данных с 1 000 000 записей
np.random.seed(42)

data = {
    "ID": np.arange(1, 1000001),
    "Age": np.random.randint(18, 90, 1000000),
    "Salary": np.random.uniform(30000, 120000, 1000000),
    "Category": np.random.choice(["A", "B", "C", "D"], 1000000),
    "Score": np.random.uniform(0, 100, 1000000),
    "Experience": np.random.randint(1, 40, 1000000),
    "Department": np.random.choice(["HR", "Engineering", "Sales", "Marketing"], 1000000)
}

df = pd.DataFrame(data)

# Отбор 7 полей
selected_fields = ["ID", "Age", "Salary", "Category", "Score", "Experience", "Department"]
df_selected = df[selected_fields]

# Рассчет характеристик для числовых полей
numerical_stats = {}
for col in ["Age", "Salary", "Score", "Experience"]:
    numerical_stats[col] = {
        "max": df_selected[col].max(),
        "min": df_selected[col].min(),
        "mean": df_selected[col].mean(),
        "sum": df_selected[col].sum(),
        "std_dev": df_selected[col].std()
    }

# Частота встречаемости для текстовых полей
categorical_stats = {}
for col in ["Category", "Department"]:
    categorical_stats[col] = df_selected[col].value_counts().to_dict()

# Сохранение статистики в JSON
statistics = {**numerical_stats, **categorical_stats}

# Преобразование в сериализуемый формат
for key, value in statistics.items():
    if isinstance(value, dict):
        for subkey, subvalue in value.items():
            if isinstance(subvalue, (np.integer, np.int32, np.int64)):
                statistics[key][subkey] = int(subvalue)
            elif isinstance(subvalue, (np.floating, np.float32, np.float64)):
                statistics[key][subkey] = float(subvalue)

statistics_path = "statistics.json"
with open(statistics_path, "w", encoding="utf-8") as json_file:
    json.dump(statistics, json_file, ensure_ascii=False, indent=4)

# Сохранение данных в различных форматах
csv_path = "selected_data.csv"
json_path = "selected_data.json"
msgpack_path = "selected_data.msgpack"
pkl_path = "selected_data.pkl"

df_selected.to_csv(csv_path, index=False)
df_selected.to_json(json_path, orient="records", lines=True)

# Сохранение в msgpack с использованием библиотеки msgpack
with open(msgpack_path, "wb") as f:
    packed = msgpack.packb(df_selected.to_dict(orient="records"))
    f.write(packed)

# Сохранение в pkl
with open(pkl_path, "wb") as pkl_file:
    pickle.dump(df_selected, pkl_file)

# Получение размеров файлов
file_sizes = {
    "csv": os.path.getsize(csv_path),
    "json": os.path.getsize(json_path),
    "msgpack": os.path.getsize(msgpack_path),
    "pkl": os.path.getsize(pkl_path)
}

print("Сохраненные файлы и их размеры (в байтах):")
for fmt, size in file_sizes.items():
    print(f"{fmt}: {size} байт")
