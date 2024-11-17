from bs4 import BeautifulSoup
import csv


with open('C:/Users/kit/Desktop/fifth_task.html', 'r', encoding='utf-8') as file:
    html_content = file.read()

soup = BeautifulSoup(html_content, 'html.parser')
table = soup.find('table', {'id': 'product-table'})
headers = [header.text for header in table.find_all('th')]
rows = []

for row in table.find_all('tr')[1:]:  # Пропуск заголовка таблицы
    rows.append([cell.text for cell in row.find_all('td')])

csv_file_path = 'C:/Users/kit/Desktop/extracted_products.csv'
with open(csv_file_path, mode='w', encoding='utf-8', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(headers)  # Запись заголовков
    writer.writerows(rows)    # Запись данных

print(f"Данные успешно сохранены в файл: {csv_file_path}")