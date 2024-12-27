import sqlite3
import csv


def connect_to_db():
    return sqlite3.connect("database.db")


def create_tables():
    commands = [
        """
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INT PRIMARY KEY,
            gender VARCHAR(10),
            ever_married VARCHAR(3),
            age INT,
            graduated VARCHAR(3),
            profession VARCHAR(50),
            work_experience INT,
            spending_score VARCHAR(10),
            family_size INT,
            var_1 VARCHAR(10),
            segmentation VARCHAR(1)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS products (
            product_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            main_category VARCHAR(50),
            sub_category VARCHAR(50),
            image TEXT,
            link TEXT,
            ratings DECIMAL(2,1),
            no_of_ratings INT,
            discount_price VARCHAR(20),
            actual_price VARCHAR(20)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS orders (
            invoice VARCHAR(20),
            stock_code VARCHAR(20),
            description TEXT,
            quantity INT,
            invoice_date TEXT,
            price DECIMAL(10, 2),
            customer_id INT,
            country VARCHAR(50),
            PRIMARY KEY (invoice, stock_code),
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        );
        """
    ]
    conn = connect_to_db()
    cur = conn.cursor()
    for command in commands:
        cur.execute(command)
    conn.commit()
    cur.close()
    conn.close()


def load_data():
    conn = connect_to_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM customers;")
    cur.execute("DELETE FROM products;")
    cur.execute("DELETE FROM orders;")
    conn.commit()

    # Load customers
    with open('Customers.csv', 'r') as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            if len(row) != 11:
                print(f"Skipping row in Customers.csv: {row} (Expected 11 columns, got {len(row)})")
                continue
            cur.execute(
                "INSERT OR IGNORE INTO customers (customer_id, gender, ever_married, age, graduated, profession, work_experience, spending_score, family_size, var_1, segmentation) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
                row
            )


    with open('product.csv', 'r') as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            if len(row) != 9:
                print(f"Skipping row in product.csv: {row} (Expected 9 columns, got {len(row)})")
                continue
            cur.execute(
                "INSERT OR IGNORE INTO products (name, main_category, sub_category, image, link, ratings, no_of_ratings, discount_price, actual_price) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
                row
            )

    with open('Orders.csv', 'r') as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            if len(row) != 8:
                print(f"Skipping row in Orders.csv: {row} (Expected 8 columns, got {len(row)})")
                continue
            cur.execute(
                "INSERT OR IGNORE INTO orders (invoice, stock_code, description, quantity, invoice_date, price, customer_id, country) VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
                row
            )

    conn.commit()
    cur.close()
    conn.close()


def execute_queries():
    queries = [

        """
        SELECT * FROM customers
        WHERE spending_score = 'High'
        ORDER BY age DESC
        LIMIT 10;
        """,

        """
        SELECT customer_id, COUNT(*) AS order_count
        FROM orders
        GROUP BY customer_id
        ORDER BY order_count DESC;
        """,

        """
        SELECT stock_code, SUM(quantity * price) AS total_revenue
        FROM orders
        GROUP BY stock_code
        ORDER BY total_revenue DESC;
        """,

        """
        SELECT main_category, AVG(ratings) AS avg_rating
        FROM products
        GROUP BY main_category;
        """,

        """
        UPDATE customers
        SET spending_score = 'High'
        WHERE work_experience > 10;
        """,

        """
        SELECT description, SUM(quantity) AS total_quantity
        FROM orders
        GROUP BY description
        ORDER BY total_quantity DESC
        LIMIT 5;
        """,

        """
        SELECT country, COUNT(*) AS total_orders
        FROM orders
        GROUP BY country
        ORDER BY total_orders DESC;
        """
    ]

    conn = connect_to_db()
    cur = conn.cursor()

    for i, query in enumerate(queries, start=1):
        cur.execute(query)
        results = cur.fetchall()
        print(f"Query {i} Results:", results)

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    create_tables()
    load_data()
    execute_queries()
