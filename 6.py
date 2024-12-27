import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Полный путь к файлу
file_path = 'yellow_tripdata_2016-03.csv'

# 1. Загрузка данных
# Читаем данные чанками, чтобы оценить объем памяти и провести дальнейшие преобразования.
data = pd.read_csv(file_path)

# 2a. Объем файла на диске
file_size = data.memory_usage(deep=True).sum() / (1024 ** 2)
print(f"Объем файла на диске: {file_size:.2f} МБ")

# 2b. Объем данных в памяти
memory_usage = data.memory_usage(deep=True).sum() / (1024 ** 2)
print(f"Объем данных в памяти: {memory_usage:.2f} МБ")

# 2c. Анализ колонок (тип данных, занимаемая память)
memory_stats = (
    data.memory_usage(deep=True)
    .reset_index()
    .rename(columns={"index": "Column", 0: "Memory_Usage"})
)
memory_stats["Memory_Usage_MB"] = memory_stats["Memory_Usage"] / (1024 ** 2)
memory_stats["Data_Type"] = memory_stats["Column"].apply(lambda x: data[x].dtype if x in data else "NA")
memory_stats = memory_stats.sort_values("Memory_Usage", ascending=False)
print(memory_stats)

# Сохраняем анализ в CSV (устраняем ошибку сохранения в JSON)
memory_stats.to_csv('memory_stats_unoptimized.csv', index=False)

# 4. Преобразование object в category
for col  in data.select_dtypes(include=['object']).columns:
    if data[col].nunique() < 0.5 * len(data):
        data[col] = data[col].astype('category')

# 5. Понижение типа int
int_columns = data.select_dtypes(include=['int']).columns
for col in int_columns:
    data[col] = pd.to_numeric(data[col], downcast='integer')

# 6. Понижение типа float
float_columns = data.select_dtypes(include=['float']).columns
for col in float_columns:
    data[col] = pd.to_numeric(data[col], downcast='float')

# 7. Повторный анализ после оптимизаций
optimized_memory_usage = data.memory_usage(deep=True).sum() / (1024 ** 2)
print(f"Объем данных после оптимизаций: {optimized_memory_usage:.2f} МБ")

# Сравнение старого и нового объема памяти
print(f"Сэкономлено памяти: {memory_usage - optimized_memory_usage:.2f} МБ")

# Сохраняем оптимизированные данные в CSV (устраняем проблему сохранения)
memory_stats_optimized = data.memory_usage(deep=True).reset_index()
memory_stats_optimized.to_csv('memory_stats_optimized.csv', index=False)
# 8. Выбор 10 колонок и чтение чанками
selected_columns = [
    "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
    "trip_distance", "fare_amount", "tip_amount", "total_amount",
    "pickup_longitude", "pickup_latitude", "dropoff_longitude"
]

chunk_size = 100000
output_file = "optimized_subset.csv"

# Создаем файл с поднабором данных
with pd.read_csv(file_path, usecols=selected_columns, chunksize=chunk_size) as reader:
    for i, chunk in enumerate(reader):
        # Преобразуем типы данных для выбранных колонок
        chunk["tpep_pickup_datetime"] = pd.to_datetime(chunk["tpep_pickup_datetime"])
        chunk["tpep_dropoff_datetime"] = pd.to_datetime(chunk["tpep_dropoff_datetime"])
        chunk["passenger_count"] = pd.to_numeric(chunk["passenger_count"], downcast="integer")
        chunk["trip_distance"] = pd.to_numeric(chunk["trip_distance"], downcast="float")
        chunk.to_csv(output_file, mode="a", index=False, header=(i == 0))

print(f"Поднабор данных сохранен в файл {output_file}")

# 9. Построение графиков на оптимизированных данных
# Функция для загрузки данных с обработкой ошибок
file_path = 'optimized_subset.csv'
data = pd.read_csv(file_path)
data['tpep_pickup_datetime'] = pd.to_datetime(data['tpep_pickup_datetime'])
data['tpep_dropoff_datetime'] = pd.to_datetime(data['tpep_dropoff_datetime'])

# 1. Линейный график: Зависимость общей стоимости поездки от дистанции
plt.figure(figsize=(10, 6))
sns.lineplot(x='trip_distance', y='total_amount', data=data, marker='o')
plt.title('Зависимость общей стоимости от дистанции', fontsize=14)
plt.xlabel('Дистанция поездки (миль)', fontsize=12)
plt.ylabel('Общая стоимость ($)', fontsize=12)
plt.grid(True)
plt.savefig('distance_vs_total_amount.png', dpi=300)
plt.show()

# 2. Столбчатая диаграмма: Средняя сумма чаевых в зависимости от количества пассажиров
avg_tip_by_passenger = data.groupby('passenger_count')['tip_amount'].mean().reset_index()

plt.figure(figsize=(10, 6))
sns.barplot(x='passenger_count', y='tip_amount', data=avg_tip_by_passenger, hue='passenger_count', palette='viridis', dodge=False, legend=False)

plt.title('Средняя сумма чаевых в зависимости от количества пассажиров', fontsize=14)
plt.xlabel('Количество пассажиров', fontsize=12)
plt.ylabel('Средняя сумма чаевых ($)', fontsize=12)
plt.grid(True)
plt.savefig('avg_tip_by_passenger.png', dpi=300)
plt.show()

# 3. Круговая диаграмма: Доли поездок по количеству пассажиров
passenger_counts = data['passenger_count'].value_counts()

plt.figure(figsize=(8, 8))
plt.pie(passenger_counts, labels=passenger_counts.index, autopct='%1.1f%%', startangle=140, colors=sns.color_palette('pastel'))
plt.title('Распределение поездок по количеству пассажиров', fontsize=14)
plt.savefig('passenger_distribution.png', dpi=300)
plt.show()


# 4. Линейный график: Зависимость средней общей стоимости от времени суток (по часам)
data['pickup_hour'] = data['tpep_pickup_datetime'].dt.hour
avg_total_by_hour = data.groupby('pickup_hour')['total_amount'].mean().reset_index()

plt.figure(figsize=(10, 6))
sns.lineplot(x='pickup_hour', y='total_amount', data=avg_total_by_hour, marker='o', color='orange')
plt.title('Средняя общая стоимость в зависимости от времени суток', fontsize=14)
plt.xlabel('Час начала поездки', fontsize=12)
plt.ylabel('Средняя общая стоимость ($)', fontsize=12)
plt.grid(True)
plt.savefig('avg_total_by_hour.png', dpi=300)
plt.show()

# 5. Столбчатая диаграмма: Распределение поездок по дальности (ближе 5 миль или дальше)
data['trip_category'] = ['< 5 миль' if x < 5 else '>= 5 миль' for x in data['trip_distance']]
trip_distance_counts = data['trip_category'].value_counts()

plt.figure(figsize=(10, 6))
sns.barplot(x=trip_distance_counts.index, y=trip_distance_counts.values, palette='rocket')
plt.title('Распределение поездок по дальности', fontsize=14)
plt.xlabel('Категория дистанции', fontsize=12)
plt.ylabel('Количество поездок', fontsize=12)
plt.grid(True)
plt.savefig('trip_distance_distribution.png', dpi=300)
plt.show()

# 6. Тепловая карта: Корреляция числовых переменных
numeric_data = data.select_dtypes(include=['number'])

plt.figure(figsize=(10, 8))
sns.heatmap(numeric_data.corr(), annot=True, fmt='.2f', cmap='coolwarm', cbar=True)
plt.title('Тепловая карта корреляции числовых переменных', fontsize=14)
plt.savefig('correlation_heatmap.png', dpi=300)
plt.show()