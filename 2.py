import sqlite3
import json
import os


def read_text(filename):
    """Читает файл формата `.text` и извлекает данные."""
    items = []
    with open(filename, "r", encoding="utf-8") as f:
        content = f.read()
        entries = content.split("=====")
        for entry in entries:
            if not entry.strip():
                continue
            data = {}
            for line in entry.strip().split("\n"):
                key, value = line.split("::")
                data[key.strip()] = value.strip()
            items.append(data)
    return items


def create_bookreviews_table(db):
    """Создает таблицу для хранения данных из файла .text."""
    cursor = db.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bookreviews (
            book_title TEXT,
            price INTEGER,
            place TEXT,
            date TEXT,
            FOREIGN KEY (book_title) REFERENCES tournament(title)
        )
    """)
    db.commit()


def insert_data_to_bookreviews(db, items):
    """Вставляет данные в таблицу bookreviews."""
    cursor = db.cursor()
    cursor.executemany("""
        INSERT INTO bookreviews (book_title, price, place, date)
        VALUES (:title, :price, :place, :date)
    """, items)
    db.commit()


def first_query(db):
    """Запрос: данные о книге с названием 'Мертвые души'."""
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT * 
        FROM bookreviews
        WHERE book_title = 'Мертвые души'
        ORDER BY date DESC
    """)
    return [dict(row) for row in res]


def second_query(db):
    """Запрос: книги, доступные offline, с их ценой."""
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT t.title, t.published_year, p.price
        FROM tournament t
        JOIN bookreviews p ON t.title = p.book_title
        WHERE p.place = 'offline'
        LIMIT 5
    """)
    return [dict(row) for row in res]


def third_query(db):
    """Запрос: суммарная стоимость книг по местам."""
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT p.place, SUM(p.price) AS total_price, COUNT(p.book_title) AS count_books
        FROM bookreviews p
        GROUP BY p.place
        ORDER BY total_price DESC
        LIMIT 5
    """)
    return [dict(row) for row in res]


def main():
    # Параметры
    text_file = "subitem.text"  # Укажите правильное имя файла
    db_file = "tournament.db"
    json_output1 = "query1_result.json"
    json_output2 = "query2_result.json"
    json_output3 = "query3_result.json"

    # Проверяем существование файла
    if not os.path.exists(text_file):
        print(f"Файл {text_file} не найден. Проверьте путь.")
        return

    # Подготовка базы данных
    db = sqlite3.connect(db_file)
    db.row_factory = sqlite3.Row

    # Чтение данных из файла
    items = read_text(text_file)

    # Создание таблицы и вставка данных
    create_bookreviews_table(db)
    insert_data_to_bookreviews(db, items)

    # Выполнение запросов
    result1 = first_query(db)
    result2 = second_query(db)
    result3 = third_query(db)

    # Сохранение результатов в файлы JSON
    with open(json_output1, 'w', encoding='utf-8') as f:
        json.dump(result1, f, ensure_ascii=False, indent=4)

    with open(json_output2, 'w', encoding='utf-8') as f:
        json.dump(result2, f, ensure_ascii=False, indent=4)

    with open(json_output3, 'w', encoding='utf-8') as f:
        json.dump(result3, f, ensure_ascii=False, indent=4)

    # Вывод результатов
    print("Результат 1:", result1[:3])
    print("Результат 2:", result2[:3])
    print("Результат 3:", result3[:3])


if __name__ == "__main__":
    main()
