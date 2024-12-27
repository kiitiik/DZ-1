import sqlite3
import json

# Шаг 1: Создание таблицы на основе данных из файла
conn = sqlite3.connect("data.db")
cursor = conn.cursor()

# Создание таблицы с учетом структуры данных
cursor.execute('''
CREATE TABLE IF NOT EXISTS buildings (
    id INTEGER PRIMARY KEY,
    name TEXT,
    street TEXT,
    city TEXT,
    zipcode TEXT,
    floors INTEGER,
    year INTEGER,
    parking BOOLEAN,
    prob_price INTEGER,
    views INTEGER
)
''')

# Шаг 2: Чтение данных из файла и добавление их в таблицу
file_path = 'C:/Users/kit/Desktop/4 звдвниеи/1-2/item.text'

with open(file_path, 'r', encoding='utf-8') as file:
    data = file.read().split('=====')

records = []
for entry in data:
    lines = entry.strip().split('\n')
    record = {}
    for line in lines:
        if '::' in line:
            key, value = line.split('::')
            record[key.strip()] = value.strip()
    if record:
        records.append((
            int(record.get('id', 0)),
            record.get('name', ''),
            record.get('street', ''),
            record.get('city', ''),
            record.get('zipcode', ''),
            int(record.get('floors', 0)),
            int(record.get('year', 0)),
            record.get('parking', 'False') == 'True',
            int(record.get('prob_price', 0)),
            int(record.get('views', 0))
        ))

cursor.executemany('''
INSERT INTO buildings (id, name, street, city, zipcode, floors, year, parking, prob_price, views)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
''', records)

conn.commit()

# Шаг 3.1: Вывод первых (VAR+10) строк, отсортированных по полю views, в файл JSON
VAR = 5  # Задайте значение VAR
rows_to_fetch = VAR + 10

cursor.execute(f'''
SELECT * FROM buildings
ORDER BY views DESC
LIMIT {rows_to_fetch}
''')

result = cursor.fetchall()

with open('sorted_views.json', 'w', encoding='utf-8') as json_file:
    json.dump([dict(zip([column[0] for column in cursor.description], row)) for row in result], json_file,
              ensure_ascii=False, indent=4)

# Шаг 3.2: Агрегатные данные для поля prob_price
cursor.execute('''
SELECT SUM(prob_price), MIN(prob_price), MAX(prob_price), AVG(prob_price)
FROM buildings
''')
aggr_result = cursor.fetchone()

print("Агрегатные данные для prob_price:", aggr_result)

# Шаг 3.3: Частота встречаемости для поля city
cursor.execute('''
SELECT city, COUNT(*) as frequency
FROM buildings
GROUP BY city
ORDER BY frequency DESC
''')
frequency_result = cursor.fetchall()

print("Частота встречаемости значений поля city:", frequency_result)

# Шаг 3.4: Фильтрация, сортировка и сохранение первых (VAR+10) строк
cursor.execute(f'''
SELECT * FROM buildings
WHERE parking = 1
ORDER BY prob_price DESC
LIMIT {rows_to_fetch}
''')

filtered_result = cursor.fetchall()

with open('filtered_parking.json', 'w', encoding='utf-8') as json_file:
    json.dump([dict(zip([column[0] for column in cursor.description], row)) for row in filtered_result], json_file,
              ensure_ascii=False, indent=4)

# Закрытие соединения
conn.close()