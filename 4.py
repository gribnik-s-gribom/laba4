import sqlite3
import pickle
import json
import os


# Чтение данных из PKL файла
def read_pkl(path):
    with open(path, "rb") as f:
        items = pickle.load(f)
        for item in items:
            item['price'] = float(item['price'])
            item['quantity'] = int(item['quantity'])
            item['isAvailable'] = bool(item['isAvailable'])
        return items


# Чтение данных из текстового файла
def read_text(path):
    with open(path, "r", encoding="utf-8") as f:
        lines = f.readlines()
        updates = []
        update = {}
        for line in lines:
            if line.strip() == "=====":
                updates.append(update)
                update = {}
                continue
            key, value = line.strip().split("::")
            update[key] = value

        for update in updates:
            if update['method'] == 'available':
                update['param'] = bool(update['param'])
            elif update['method'] != 'remove':
                update['param'] = float(update['param'])

        return updates


# Создание таблицы
def create_product_table(db):
    cursor = db.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS product (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            price FLOAT,
            quantity INTEGER,
            fromCity TEXT,
            isAvailable INTEGER,
            views INTEGER,
            version INTEGER DEFAULT 0
        )
    """)
    db.commit()


# Вставка данных
def insert_data(db, items):
    cursor = db.cursor()
    cursor.executemany("""
        INSERT OR IGNORE INTO product (name, price, quantity, fromCity, isAvailable, views)
        VALUES (:name, :price, :quantity, :fromCity, :isAvailable, :views)
    """, items)
    db.commit()


# Обработка обновлений
def handle_updates(db, updates):
    cursor = db.cursor()
    for update in updates:
        method = update['method']
        name = update['name']
        param = update.get('param')

        if method == 'remove':
            cursor.execute("DELETE FROM product WHERE name = ?", (name,))
        elif method == 'price_abs':
            cursor.execute("""
                UPDATE product
                SET price = MAX(0, price + ?), version = version + 1
                WHERE name = ?
            """, (param, name))
        elif method == 'quantity_add':
            cursor.execute("""
                UPDATE product
                SET quantity = MAX(0, quantity + ?), version = version + 1
                WHERE name = ?
            """, (param, name))
        elif method == 'available':
            cursor.execute("""
                UPDATE product
                SET isAvailable = ?, version = version + 1
                WHERE name = ?
            """, (param, name))
    db.commit()


# Выполнение запросов
def execute_queries(db):
    results = {}

    cursor = db.cursor()
    results['top_updated'] = cursor.execute("""
        SELECT name, version FROM product
        ORDER BY version DESC
        LIMIT 10
    """).fetchall()

    results['price_analysis'] = cursor.execute("""
        SELECT
            fromCity,
            COUNT(*) AS count,
            SUM(price) AS total_price,
            MIN(price) AS min_price,
            MAX(price) AS max_price,
            AVG(price) AS avg_price
        FROM product
        GROUP BY fromCity
    """).fetchall()

    results['quantity_analysis'] = cursor.execute("""
        SELECT
            fromCity,
            COUNT(*) AS count,
            SUM(quantity) AS total_quantity,
            MIN(quantity) AS min_quantity,
            MAX(quantity) AS max_quantity,
            AVG(quantity) AS avg_quantity
        FROM product
        GROUP BY fromCity
    """).fetchall()

    results['custom_query'] = cursor.execute("""
        SELECT name, quantity FROM product
        WHERE quantity > 10
        ORDER BY quantity DESC
    """).fetchall()

    return results


# Основная функция
def main():
    db_file = "products.db"
    pkl_file = "_product_data.pkl"
    text_file = "_update_data.text"
    output_file = "query_results.json"

    if not os.path.exists(pkl_file) or not os.path.exists(text_file):
        print("Файлы не найдены. Проверьте пути.")
        return

    db = sqlite3.connect(db_file)
    db.row_factory = sqlite3.Row
    create_product_table(db)

    items = read_pkl(pkl_file)
    updates = read_text(text_file)
    insert_data(db, items)
    handle_updates(db, updates)

    results = execute_queries(db)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump({k: [dict(row) for row in v] for k, v in results.items()}, f, ensure_ascii=False, indent=4)

    print(f"Результаты сохранены в файл: {output_file}")


if __name__ == "__main__":
    main()
