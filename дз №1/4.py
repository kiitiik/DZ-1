import csv

def read_csv(path):
    data = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            data.append({
                'product_id': int(row['product_id']),
                'name': row['name'],
                'price': float(row['price']),
                'quantity': int(row['quantity']),
                'category': row['category'],
                'description': row['description'],
                #'production_date': row['production_date'],
                'expiration_date': row['expiration_date'],
                'rating': float(row['rating']),
                'status': row['status'],
            })
    return data


# Загрузка исходного CSV файла
data = read_csv('C:/Users/kit/Desktop/fourth_task.txt')

size = len(data)
avg_price = 0
max_price = data[0]['price']
min_quantity = data[0]['quantity']

filt_df = []
for item in data:
    avg_price += item['price']

    if max_price < item['price']:
        max_price = item['price']

    if min_quantity > item['quantity']:
        min_quantity = item['quantity']

    if item['status'] == 'New':
        filt_df.append(item)
avg_price /= size

with open("C:/Users/kit/Desktop/4.txt", "w", encoding="utf-8") as f:
    f.write(f"{avg_price}" + " ")
    f.write(f"{max_price}" + " ")
    f.write(f"{min_quantity}" + " ")

with open("C:/Users/kit/Desktop/4.csv", "w", encoding="utf-8") as f:
    writer = csv.DictWriter(f, filt_df[0].keys())
    writer.writeheader()
    for row in filt_df:
        writer.writerow(row)
