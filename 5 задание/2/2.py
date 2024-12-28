import pandas as pd
from pymongo import MongoClient
import json
# Путь к JSON-файлу
file_path = "C:\\Users\\kit\\Desktop\\5 задание\\2\\task_2_item.json"

# Чтение данных из файла
with open(file_path, "r", encoding="utf-8") as file:
    data = json.load(file)  # Это уже список словарей

# Подключение к MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["task_database"]
collection = db["task_collection"]

# Добавление данных в коллекцию MongoDB
collection.insert_many(data)
# Функция для сохранения результатов в JSON
def save_to_json(filename, data):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

# Запросы и сохранение результатов

# 1. Минимальная, средняя, максимальная salary
result_1 = collection.aggregate([
    {"$group": {
        "_id": None,
        "min_salary": {"$min": "$salary"},
        "avg_salary": {"$avg": "$salary"},
        "max_salary": {"$max": "$salary"}
    }}
])
save_to_json("result_1.json", list(result_1))

# 2. Количество данных по представленным профессиям
result_2 = collection.aggregate([
    {"$group": {
        "_id": "$job",
        "count": {"$sum": 1}
    }}
])
save_to_json("result_2.json", list(result_2))

# 3. Минимальная, средняя, максимальная salary по городам
result_3 = collection.aggregate([
    {"$group": {
        "_id": "$city",
        "min_salary": {"$min": "$salary"},
        "avg_salary": {"$avg": "$salary"},
        "max_salary": {"$max": "$salary"}
    }}
])
save_to_json("result_3.json", list(result_3))

# 4. Минимальная, средняя, максимальная salary по профессиям
result_4 = collection.aggregate([
    {"$group": {
        "_id": "$job",
        "min_salary": {"$min": "$salary"},
        "avg_salary": {"$avg": "$salary"},
        "max_salary": {"$max": "$salary"}
    }}
])
save_to_json("result_4.json", list(result_4))

# 5. Минимальный, средний, максимальный возраст по городам
result_5 = collection.aggregate([
    {"$group": {
        "_id": "$city",
        "min_age": {"$min": "$age"},
        "avg_age": {"$avg": "$age"},
        "max_age": {"$max": "$age"}
    }}
])
save_to_json("result_5.json", list(result_5))

# 6. Минимальный, средний, максимальный возраст по профессиям
result_6 = collection.aggregate([
    {"$group": {
        "_id": "$job",
        "min_age": {"$min": "$age"},
        "avg_age": {"$avg": "$age"},
        "max_age": {"$max": "$age"}
    }}
])
save_to_json("result_6.json", list(result_6))

# 7. Максимальная зарплата при минимальном возрасте
result_7 = collection.aggregate([
    {"$group": {
        "_id": None,
        "min_age": {"$min": "$age"}
    }},
    {"$lookup": {
        "from": "task_collection",
        "localField": "min_age",
        "foreignField": "age",
        "as": "matching_docs"
    }},
    {"$unwind": "$matching_docs"},
    {"$group": {
        "_id": None,
        "max_salary": {"$max": "$matching_docs.salary"}
    }}
])
save_to_json("result_7.json", list(result_7))

# 8. Минимальная зарплата при максимальном возрасте
result_8 = collection.aggregate([
    {"$group": {
        "_id": None,
        "max_age": {"$max": "$age"}
    }},
    {"$lookup": {
        "from": "task_collection",
        "localField": "max_age",
        "foreignField": "age",
        "as": "matching_docs"
    }},
    {"$unwind": "$matching_docs"},
    {"$group": {
        "_id": None,
        "min_salary": {"$min": "$matching_docs.salary"}
    }}
])
save_to_json("result_8.json", list(result_8))

# 9. Возраст по городам при salary > 50 000, отсортировано по убыванию avg_age
result_9 = collection.aggregate([
    {"$match": {"salary": {"$gt": 50000}}},
    {"$group": {
        "_id": "$city",
        "min_age": {"$min": "$age"},
        "avg_age": {"$avg": "$age"},
        "max_age": {"$max": "$age"}
    }},
    {"$sort": {"avg_age": -1}}
])
save_to_json("result_9.json", list(result_9))

# 10. Минимальная, средняя, максимальная salary в диапазонах age
result_10 = collection.aggregate([
    {"$match": {"$or": [
        {"age": {"$gt": 18, "$lt": 25}},
        {"age": {"$gt": 50, "$lt": 65}}
    ]}},
    {"$group": {
        "_id": None,
        "min_salary": {"$min": "$salary"},
        "avg_salary": {"$avg": "$salary"},
        "max_salary": {"$max": "$salary"}
    }}
])
save_to_json("result_10.json", list(result_10))

# 11. Произвольный запрос: города с более чем 5 записями, отсортировано по количеству
result_11 = collection.aggregate([
    {"$group": {
        "_id": "$city",
        "count": {"$sum": 1}
    }},
    {"$match": {"count": {"$gt": 5}}},
    {"$sort": {"count": -1}}
])
save_to_json("result_11.json", list(result_11))

print("Результаты всех запросов сохранены в JSON-файлы.")