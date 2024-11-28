import pickle

from AppData.Local.Programs.Python.Python310.Lib import json

with open('C:/Users/kit/Desktop/fourth_task_products.pkl', 'rb') as pkl_file:
    products = pickle.load(pkl_file)

# Считаем данные о новых ценах из JSON-файла
with open('C:/Users/kit/Desktop/fourth_task_updates.json', 'r', encoding='utf-8') as json_file:
    price_updates = json.load(json_file)


# Функция для обновления цены в зависимости от метода
def update_price(current_price, method, param):
    if method == "add":
        return current_price + param
    elif method == "sub":
        return current_price - param
    elif method == "percent+":
        return current_price * (1 + param)
    elif method == "percent-":
        return current_price * (1 - param)


# Обновляем цены товаров
for update in price_updates:
    name = update["name"]
    method = update["method"]
    param = update["param"]

    if name in products:
        current_price = products[name]["price"]
        new_price = update_price(current_price, method, param)
        products[name]["price"] = round(new_price, 2)

# Сохраним модифицированные данные обратно в pkl-файл
updated_pkl_path = 'C:/Users/kit/Desktop/updated_products.pkl'
with open(updated_pkl_path, 'wb') as pkl_file:
    pickle.dump(products, pkl_file)

updated_pkl_path, products