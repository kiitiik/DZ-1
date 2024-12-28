import pandas as pd
from pymongo import MongoClient
import json
# Загружаем файл для просмотра содержимого
file_path = 'task_1_item.csv'
data = pd.read_csv(file_path, delimiter=';')


# Подключаемся к MongoDB (локально, стандартный порт)
client = MongoClient("mongodb://localhost:27017/")
db = client['task_database']  # Создаем/выбираем базу данных
collection = db['task_collection']  # Создаем/выбираем коллекцию

# Преобразуем DataFrame в список словарей и вставляем в коллекцию
records = data.to_dict(orient='records')
collection.insert_many(records)

# Запрос 1: Первые 10 записей, отсортированные по убыванию salary
query1 = list(collection.find().sort("salary", -1).limit(10))
with open("query1.json", "w") as f:
    json.dump(query1, f, default=str)

# Запрос 2: Первые 15 записей с age < 30, отсортированные по убыванию salary
query2 = list(collection.find({"age": {"$lt": 30}}).sort("salary", -1).limit(15))
with open("query2.json", "w") as f:
    json.dump(query2, f, default=str)

# Запрос 3: Первые 10 записей по сложному предикату
city = "ПримерныйГород"
professions = ["ПримернаяПрофессия1", "ПримернаяПрофессия2", "ПримернаяПрофессия3"]
query3 = list(
    collection.find(
        {"$and": [{"city": city}, {"profession": {"$in": professions}}]}
    ).sort("age", 1).limit(10)
)
with open("query3.json", "w") as f:
    json.dump(query3, f, default=str)

# Запрос 4: Количество записей по фильтру
age_range = {"$gte": 25, "$lte": 35}
year_range = {"$gte": 2019, "$lte": 2022}
salary_filter = {"$or": [{"salary": {"$gt": 50000, "$lte": 75000}}, {"salary": {"$gt": 125000, "$lt": 150000}}]}
query4_count = collection.count_documents(
    {"$and": [{"age": age_range}, {"year": year_range}, salary_filter]}
)
with open("query4.json", "w") as f:
    json.dump({"count": query4_count}, f)

# Завершение
print("Все запросы выполнены и сохранены в JSON.")