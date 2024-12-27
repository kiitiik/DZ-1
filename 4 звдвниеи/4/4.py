import sqlite3
import json
import pickle

# Создание базы данных и подключение
conn = sqlite3.connect('C:/Users/kit/Desktop/4 звдвниеи/4/products.db')
cursor = conn.cursor()

# Создание таблицы для товаров
cursor.execute('''
CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE,
    price REAL,
    quantity INTEGER,
    category TEXT,
    fromCity TEXT,
    isAvailable BOOLEAN,
    views INTEGER,
    update_count INTEGER DEFAULT 0
)
''')

# Загрузка данных из файла _product_data.json
with open('_product_data.json', 'r', encoding='utf-8') as f:
    product_data = json.load(f)

# Вставка данных в таблицу
for product in product_data:
    cursor.execute('''
    INSERT OR IGNORE INTO products (name, price, quantity, category, fromCity, isAvailable, views, update_count)
    VALUES (?, ?, ?, ?, ?, ?, ?, 0)
    ''', (
        product.get('name'),
        product.get('price'),
        product.get('quantity'),
        product.get('category'),
        product.get('fromCity'),
        product.get('isAvailable'),
        product.get('views')
    ))

conn.commit()

# Загрузка данных обновлений из _update_data.pkl
with open('C:/Users/kit/Desktop/4 звдвниеи/4/_update_data.pkl', 'rb') as f:
    updates = pickle.load(f)

# Применение обновлений с использованием транзакций
def apply_update(update):
    try:
        method = update.get('method')
        param = update.get('param')
        name = update.get('name')

        if not method or param is None or not name:
            print(f"Invalid update format: {update}")
            return

        with conn:
            if method == 'price_percent':
                # Изменение цены на процент
                cursor.execute('''
                UPDATE products
                SET price = price * (1 + ?), update_count = update_count + 1
                WHERE name = ? AND price * (1 + ?) >= 0
                ''', (param, name, param))

            elif method == 'price_abs':
                # Абсолютное изменение цены
                cursor.execute('''
                UPDATE products
                SET price = price + ?, update_count = update_count + 1
                WHERE name = ? AND price + ? >= 0
                ''', (param, name, param))

            elif method == 'quantity_sub':
                # Уменьшение остатков
                cursor.execute('''
                UPDATE products
                SET quantity = quantity + ?, update_count = update_count + 1
                WHERE name = ? AND quantity + ? >= 0
                ''', (param, name, param))

            elif method == 'quantity_add':
                # Увеличение остатков
                cursor.execute('''
                UPDATE products
                SET quantity = quantity + ?, update_count = update_count + 1
                WHERE name = ?
                ''', (param, name))

            elif method == 'available':
                # Изменение доступности товара
                cursor.execute('''
                UPDATE products
                SET isAvailable = ?, update_count = update_count + 1
                WHERE name = ?
                ''', (param, name))

            elif method == 'remove':
                # Удаление товара из каталога
                cursor.execute('''
                DELETE FROM products WHERE name = ?
                ''', (name,))

            else:
                print(f"Unknown method: {method}")

    except sqlite3.Error as e:
        print(f"Error applying update: {update}, Error: {e}")


for update in updates:
    apply_update(update)

# Запросы и сохранение результатов
# 1. Топ-10 самых обновляемых товаров
cursor.execute('''
SELECT name, update_count FROM products
ORDER BY update_count DESC
LIMIT 10
''')
top_updated_products = cursor.fetchall()
with open('запрос_1.txt', 'w', encoding='utf-8') as f:
    for row in top_updated_products:
        f.write(f"{row}\n")

# 2. Анализ цен товаров по группам
cursor.execute('''
SELECT category, COUNT(*), SUM(price), MIN(price), MAX(price), AVG(price)
FROM products
GROUP BY category
''')
price_analysis = cursor.fetchall()
with open('запрос_2.txt', 'w', encoding='utf-8') as f:
    for row in price_analysis:
        f.write(f"{row}\n")

# 3. Анализ остатков товаров по группам
cursor.execute('''
SELECT category, COUNT(*), SUM(quantity), MIN(quantity), MAX(quantity), AVG(quantity)
FROM products
GROUP BY category
''')
quantity_analysis = cursor.fetchall()
with open('запрос_3.txt', 'w', encoding='utf-8') as f:
    for row in quantity_analysis:
        f.write(f"{row}\n")

# 4. Произвольный запрос: Товары с остатком <= 50
cursor.execute('''
SELECT name, quantity FROM products WHERE quantity <= 50
''')
low_stock_products = cursor.fetchall()
with open('запрос_4.txt', 'w', encoding='utf-8') as f:
    for row in low_stock_products:
        f.write(f"{row}\n")

conn.close()
