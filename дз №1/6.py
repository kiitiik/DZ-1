import requests


response = requests.get('https://jsonplaceholder.typicode.com/posts')
data = response.json()

html = '<html><body><table border="1">'
html += '<tr><th>UserId</th><th>ID</th><th>Title</th><th>Body</th></tr>'
for item in data:
    html += f"<tr><td>{item['userId']}</td><td>{item['id']}</td><td>{item['title']}</td><td>{item['body']}</td></tr>"
html += '</table></body></html>'

with open('output.html', 'w') as file:
    file.write(html)

print("HTML сохранён в output.html")