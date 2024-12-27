import sqlite3
import json

# Шаг 1: Создание таблиц на основе данных из файлов
conn = sqlite3.connect("data.db")
cursor = conn.cursor()

# Создание таблицы buildings
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

# Создание таблицы reviews
cursor.execute('''
CREATE TABLE IF NOT EXISTS reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    building_name TEXT,
    rating REAL,
    convenience INTEGER,
    security INTEGER,
    functionality INTEGER,
    comment TEXT,
    FOREIGN KEY (building_name) REFERENCES buildings(name)
)
''')

# Шаг 2: Чтение данных из первого файла и добавление их в таблицу buildings
file_path_buildings = 'C:/Users/kit/Desktop/4 звдвниеи/1-2/item.text'

with open(file_path_buildings, 'r', encoding='utf-8') as file:
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
cursor.execute("DELETE FROM buildings")
cursor.executemany('''
INSERT INTO buildings (id, name, street, city, zipcode, floors, year, parking, prob_price, views)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
''', records)

# Шаг 3: Чтение данных из второго файла и добавление их в таблицу reviews
file_path_reviews = 'C:/Users/kit/Desktop/4 звдвниеи/1-2/subitem.json'

with open(file_path_reviews, 'r', encoding='utf-8') as file:
    reviews_data = json.load(file)

review_records = [
    (
        review.get('name', ''),
        review.get('rating', 0.0),
        review.get('convenience', 0),
        review.get('security', 0),
        review.get('functionality', 0),
        review.get('comment', '')
    )
    for review in reviews_data
]

cursor.executemany('''
INSERT INTO reviews (building_name, rating, convenience, security, functionality, comment)
VALUES (?, ?, ?, ?, ?, ?)
''', review_records)

conn.commit()

# Шаг 4: Реализация запросов, использующих обе таблицы

# Запрос 1: Объединение с фильтрацией по рейтингу
cursor.execute('''
SELECT b.name, b.city, r.rating, r.comment
FROM buildings b
JOIN reviews r ON b.name = r.building_name
WHERE r.rating > 4.0
ORDER BY r.rating DESC
''')
high_rating_buildings = cursor.fetchall()
with open('запрос_1.json', 'w', encoding='utf-8') as file:
    json.dump(high_rating_buildings, file, ensure_ascii=False, indent=4)

# Запрос 2: Подсчет средней оценки для зданий с определенным количеством этажей
cursor.execute('''
SELECT b.floors, AVG(r.rating) as avg_rating
FROM buildings b
JOIN reviews r ON b.name = r.building_name
GROUP BY b.floors
ORDER BY b.floors
''')
average_ratings_by_floors = cursor.fetchall()
with open('запрос_2.json', 'w', encoding='utf-8') as file:
    json.dump(average_ratings_by_floors, file, ensure_ascii=False, indent=4)

# Запрос 3: Выбор комментариев для зданий с высокой стоимостью
cursor.execute('''
SELECT b.name, b.prob_price, r.comment
FROM buildings b
JOIN reviews r ON b.name = r.building_name
WHERE b.prob_price > 500000000
ORDER BY b.prob_price DESC
''')
expensive_building_comments = cursor.fetchall()
with open('запрос_3.json', 'w', encoding='utf-8') as file:
    json.dump(expensive_building_comments, file, ensure_ascii=False, indent=4)

# Закрытие соединения
conn.close()
