import json
import msgpack

from AppData.Local.Programs.Python.Python310.Lib import os

with open('C:/Users/kit/Desktop/third_task.json', 'r', encoding='utf-8') as file:
    data = json.load(file)

aggregated_data = {}

for item in data:
    name = item['name']
    price = item['price']

    if name not in aggregated_data:
        aggregated_data[name] = {'prices': []}

    aggregated_data[name]['prices'].append(price)

result = {}
for name, info in aggregated_data.items():
    prices = info['prices']
    result[name] = {
        'average_price': sum(prices) / len(prices),
        'max_price': max(prices),
        'min_price': min(prices)
    }

json_path = 'C:/Users/kit/Desktop/aggregated_data.json'
with open(json_path, 'w', encoding='utf-8') as json_file:
    json.dump(result, json_file, ensure_ascii=False, indent=4)

msgpack_path = 'C:/Users/kit/Desktop/aggregated_data.msgpack'
with open(msgpack_path, 'wb') as msgpack_file:
    msgpack.pack(result, msgpack_file)


size_json = os.path.getsize(json_path)
size_msgpack = os.path.getsize(msgpack_path)

(size_json, size_msgpack), json_path, msgpack_path