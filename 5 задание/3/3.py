import pymongo
import re
import json
from datetime import datetime

# Подключение к MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["my_database"]  # Укажите имя базы данных
collection = db["jobs"]  # Укажите имя коллекции

# Функция для записи логов в JSON
log_file = "query_logs.json"
logs = []

def log_query(description, query, result=None):
    logs.append({
        "timestamp": datetime.now().isoformat(),
        "description": description,
        "query": query,
        "result": result
    })
    with open(log_file, "w", encoding="utf-8") as f:
        json.dump(logs, f, ensure_ascii=False, indent=4)

# 1. Чтение данных из файла
def parse_file(file_path):
    data = []
    with open(file_path, "r", encoding="utf-8") as file:
        entry = {}
        for line in file:
            line = line.strip()
            if line == "=====":
                if entry:
                    data.append(entry)
                    entry = {}
            else:
                key, value = re.split(r"::", line)
                if key == "salary" or key == "age":
                    value = int(value)
                entry[key] = value
        if entry:
            data.append(entry)
    return data

file_path = "task_3_item.text"  # Замените на путь к вашему файлу
data = parse_file(file_path)

# 2. Загрузка данных в MongoDB
collection.delete_many({})  # Очистка коллекции перед вставкой
result = collection.insert_many(data)
log_query("Импорт данных в MongoDB", {"operation": "insert_many"}, {"inserted_count": len(result.inserted_ids)})
print(f"Данные успешно загружены. Вставлено {len(data)} документов.")

# 3. Выполнение запросов

# Удаление документов по предикату: salary < 25000 или salary > 175000
delete_query = {
    "$or": [
        {"salary": {"$lt": 25000}},
        {"salary": {"$gt": 175000}}
    ]
}
result = collection.delete_many(delete_query)
log_query("Удаление документов по предикату", delete_query, {"deleted_count": result.deleted_count})

# Увеличение возраста (age) на 1
update_query = {}
update_action = {"$inc": {"age": 1}}
result = collection.update_many(update_query, update_action)
log_query("Увеличение возраста на 1 для всех документов", update_query, {"modified_count": result.modified_count})

# Поднятие зарплаты на 5% для случайных профессий
professions = ["Программист", "Врач", "Менеджер"]  # Произвольные профессии
update_query = {"job": {"$in": professions}}
update_action = {"$mul": {"salary": 1.05}}
result = collection.update_many(update_query, update_action)
log_query("Поднятие зарплаты на 5% для случайных профессий", update_query, {"modified_count": result.modified_count})

# Поднятие зарплаты на 7% для случайных городов
cities = ["Москва", "Санкт-Петербург", "Алма-Ата"]  # Произвольные города
update_query = {"city": {"$in": cities}}
update_action = {"$mul": {"salary": 1.07}}
result = collection.update_many(update_query, update_action)
log_query("Поднятие зарплаты на 7% для случайных городов", update_query, {"modified_count": result.modified_count})

# Поднятие зарплаты на 10% по сложному предикату
update_query = {
    "$and": [
        {"city": "Москва"},  # Произвольный город
        {"job": {"$in": ["Программист", "Менеджер"]}},  # Произвольные профессии
        {"age": {"$gte": 30, "$lte": 50}}  # Произвольный диапазон возраста
    ]
}
update_action = {"$mul": {"salary": 1.10}}
result = collection.update_many(update_query, update_action)
log_query("Поднятие зарплаты на 10% по сложному предикату", update_query, {"modified_count": result.modified_count})

# Удаление записей по случайному предикату
delete_query = {"age": {"$gt": 60}}  # Удаление людей старше 60 лет
result = collection.delete_many(delete_query)
log_query("Удаление записей по случайному предикату", delete_query, {"deleted_count": result.deleted_count})

print("Запросы выполнены успешно. Логи сохранены в query_logs.json.")
