import pandas as pd
from pymongo import MongoClient
import json

# Функция для сохранения результата в файл JSON
def save_to_json(filename, data):
    # Преобразуем ObjectId в строку
    for doc in data:
        if "_id" in doc:
            doc["_id"] = str(doc["_id"])
    with open(filename, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=4)

# Подключение к MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["business_data"]

# Очистка коллекций перед добавлением
db.customers.delete_many({})
db.orders.delete_many({})

# Загрузка данных из файлов
customers_file = "Customers.csv"
orders_file = "Orders.csv"

customers_data = pd.read_csv(customers_file)
orders_data = pd.read_csv(orders_file)

# Добавление данных в MongoDB
db.customers.insert_many(customers_data.to_dict(orient="records"))
db.orders.insert_many(orders_data.to_dict(orient="records"))

print("Данные успешно загружены в MongoDB.")

# -------------------------
# Категория 1: Выборка
# -------------------------
# 1. Найти клиентов с возрастом больше 30
query1 = list(db.customers.find({"Age": {"$gt": 30}}))
print("Запрос 1 (Клиенты с возрастом > 30):", query1)
save_to_json("query1_customers_age_gt_30.json", query1)

# 2. Найти клиентов с сегментацией "A"
query2 = list(db.customers.find({"Segmentation": "A"}))
print("Запрос 2 (Клиенты с сегментацией 'A'):", query2)
save_to_json("query2_customers_segmentation_a.json", query2)

# 3. Найти заказы в декабре 2009 года
query3 = list(db.orders.find({"InvoiceDate": {"$regex": "^2009-12"}}))
print("Запрос 3 (Заказы в декабре 2009):", query3)
save_to_json("query3_orders_december_2009.json", query3)

# 4. Найти заказы с ценой выше 5
query4 = list(db.orders.find({"Price": {"$gt": 5}}))
print("Запрос 4 (Заказы с ценой > 5):", query4)
save_to_json("query4_orders_price_gt_5.json", query4)

# 5. Найти всех клиентов, которые не женаты
query5 = list(db.customers.find({"Ever_Married": "No"}))
print("Запрос 5 (Клиенты, не женатые):", query5)
save_to_json("query5_customers_not_married.json", query5)

# -------------------------
# Категория 2: Агрегация
# -------------------------
# 1. Средний возраст клиентов
agg1 = list(db.customers.aggregate([
    {"$group": {"_id": None, "avg_age": {"$avg": "$Age"}}}
]))
print("Агрегация 1 (Средний возраст клиентов):", agg1)
save_to_json("agg1_avg_customer_age.json", agg1)

# 2. Количество клиентов по профессии
agg2 = list(db.customers.aggregate([
    {"$group": {"_id": "$Profession", "count": {"$sum": 1}}}
]))
print("Агрегация 2 (Количество клиентов по профессии):", agg2)
save_to_json("agg2_customers_by_profession.json", agg2)

# 3. Общая сумма заказов по странам
agg3 = list(db.orders.aggregate([
    {"$group": {"_id": "$Country", "total_spent": {"$sum": {"$multiply": ["$Quantity", "$Price"]}}}}
]))
print("Агрегация 3 (Общая сумма заказов по странам):", agg3)
save_to_json("agg3_total_spent_by_country.json", agg3)

# 4. Максимальная цена товаров по странам
agg4 = list(db.orders.aggregate([
    {"$group": {"_id": "$Country", "max_price": {"$max": "$Price"}}}
]))
print("Агрегация 4 (Максимальная цена товаров по странам):", agg4)
save_to_json("agg4_max_price_by_country.json", agg4)

# 5. Средний размер семьи клиентов
agg5 = list(db.customers.aggregate([
    {"$group": {"_id": None, "avg_family_size": {"$avg": "$Family_Size"}}}
]))
print("Агрегация 5 (Средний размер семьи):", agg5)
save_to_json("agg5_avg_family_size.json", agg5)

# -------------------------
# Категория 3: Обновление/Удаление
# -------------------------
# 1. Увеличить возраст всех клиентов на 1 год
db.customers.update_many({}, {"$inc": {"Age": 1}})
print("Обновление 1 (Возраст увеличен на 1 год): Выполнено.")

# 2. Удалить заказы с количеством товаров менее 10
db.orders.delete_many({"Quantity": {"$lt": 10}})
print("Удаление 2 (Заказы с количеством < 10): Выполнено.")

# 3. Увеличить зарплату клиентов в профессии "Engineer" на 5%
db.customers.update_many({"Profession": "Engineer"}, {"$mul": {"Salary": 1.05}})
print("Обновление 3 (Зарплата 'Engineer' увеличена на 5%): Выполнено.")

# 4. Увеличить зарплату клиентов в городе "London" на 7%
db.customers.update_many({"City": "London"}, {"$mul": {"Salary": 1.07}})
print("Обновление 4 (Зарплата в 'London' увеличена на 7%): Выполнено.")

# 5. Удалить клиентов с семейным размером меньше 2
db.customers.delete_many({"Family_Size": {"$lt": 2}})
print("Удаление 5 (Клиенты с Family_Size < 2): Выполнено.")
