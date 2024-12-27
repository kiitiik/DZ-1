import sqlite3
import json
import msgpack

# Шаг 1: Создание соединения с базой данных
conn = sqlite3.connect("data.db")
cursor = conn.cursor()

# Создание объединенной таблицы
cursor.execute('''
CREATE TABLE IF NOT EXISTS tracks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    artist TEXT,
    song TEXT,
    duration_ms INTEGER,
    year INTEGER,
    tempo REAL,
    genre TEXT,
    instrumentalness REAL,
    explicit BOOLEAN,
    loudness REAL
)
''')

# Шаг 2: Чтение данных из первого файла (msgpack)
file_path_msgpack = 'C:/Users/kit/Desktop/4 звдвниеи/3/_part_1.msgpack'
with open(file_path_msgpack, 'rb') as file:
    msgpack_data = msgpack.unpackb(file.read(), raw=False)

records_msgpack = [
    (
        track.get('artist', ''),
        track.get('song', ''),
        track.get('duration_ms', 0),
        track.get('year', 0),
        track.get('tempo', 0.0),
        track.get('genre', ''),
        track.get('instrumentalness', 0.0),
        track.get('explicit', False),
        track.get('loudness', 0.0)
    )
    for track in msgpack_data
]

# Шаг 3: Чтение данных из второго файла (text)
file_path_text = 'C:/Users/kit/Desktop/4 звдвниеи/3/_part_2.text'
with open(file_path_text, 'r', encoding='utf-8') as file:
    text_data = file.read().split('=====')

records_text = []
for entry in text_data:
    lines = entry.strip().split('\n')
    record = {}
    for line in lines:
        if '::' in line:
            key, value = line.split('::')
            record[key.strip()] = value.strip()
    if record:
        records_text.append((
            record.get('artist', ''),
            record.get('song', ''),
            int(record.get('duration_ms', 0)),
            int(record.get('year', 0)),
            float(record.get('tempo', 0.0)),
            record.get('genre', ''),
            float(record.get('instrumentalness', 0.0)),
            record.get('explicit', 'False') == 'True',
            float(record.get('loudness', 0.0))
        ))

# Шаг 4: Запись данных в таблицу
cursor.executemany('''
INSERT INTO tracks (artist, song, duration_ms, year, tempo, genre, instrumentalness, explicit, loudness)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
''', records_msgpack + records_text)

conn.commit()

# Шаг 5: Реализация запросов
VAR = 5
rows_to_fetch = VAR + 10

# Запрос 1: Вывод первых (VAR+10) строк, отсортированных по полю duration_ms, в файл JSON
cursor.execute(f'''
SELECT * FROM tracks
ORDER BY duration_ms DESC
LIMIT {rows_to_fetch}
''')
result_1 = cursor.fetchall()
with open('запрос_1.json', 'w', encoding='utf-8') as file:
    json.dump([dict(zip([column[0] for column in cursor.description], row)) for row in result_1], file, ensure_ascii=False, indent=4)

# Запрос 2: Агрегатные данные для поля loudness
cursor.execute('''
SELECT SUM(loudness), MIN(loudness), MAX(loudness), AVG(loudness)
FROM tracks
''')
aggr_result = cursor.fetchone()
with open('запрос_2.json', 'w', encoding='utf-8') as file:
    json.dump({"sum": aggr_result[0], "min": aggr_result[1], "max": aggr_result[2], "avg": aggr_result[3]}, file, ensure_ascii=False, indent=4)

# Запрос 3: Частота встречаемости для поля genre
cursor.execute('''
SELECT genre, COUNT(*) as frequency
FROM tracks
GROUP BY genre
ORDER BY frequency DESC
''')
frequency_result = cursor.fetchall()
with open('запрос_3.json', 'w', encoding='utf-8') as file:
    json.dump([{"genre": row[0], "frequency": row[1]} for row in frequency_result], file, ensure_ascii=False, indent=4)

# Запрос 4: Фильтрация, сортировка и сохранение первых (VAR+15) строк
rows_to_fetch_filtered = VAR + 15
cursor.execute(f'''
SELECT * FROM tracks
WHERE explicit = 1
ORDER BY tempo ASC
LIMIT {rows_to_fetch_filtered}
''')
result_4 = cursor.fetchall()
with open('запрос_4.json', 'w', encoding='utf-8') as file:
    json.dump([dict(zip([column[0] for column in cursor.description], row)) for row in result_4], file, ensure_ascii=False, indent=4)

# Закрытие соединения
conn.close()
