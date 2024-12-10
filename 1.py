import sqlite3
import csv
import json
import os


def load_csv(filename):
    """Загружает данные из CSV-файла."""
    items = []
    with open(filename, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            item = {
                'title': row['title'],
                'author': row['author'],
                'genre': row['genre'],
                'pages': int(row['pages']),
                'published_year': int(row['published_year']),
                'isbn': row['isbn'],
                'rating': float(row['rating']),
                'views': int(row['views'])
            }
            items.append(item)
    return items


def create_table(db):
    """Создает таблицу для хранения данных."""
    cursor = db.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bookreviews (
            title TEXT, 
            author TEXT, 
            genre TEXT, 
            pages INTEGER, 
            published_year INTEGER, 
            isbn TEXT, 
            rating REAL, 
            views INTEGER
        )
    """)
    db.commit()


def insert_data(db, items):
    """Вставляет данные в таблицу."""
    cursor = db.cursor()
    cursor.executemany("""
        INSERT INTO bookreviews (title, author, genre, pages, published_year, isbn, rating, views)
        VALUES (:title, :author, :genre, :pages, :published_year, :isbn, :rating, :views)
    """, items)
    db.commit()


def first_query(db, limit):
    """Запрос для вывода первых N строк, отсортированных по числовому полю (views)."""
    cursor = db.cursor()
    res = cursor.execute(f"""
        SELECT * 
        FROM bookreviews
        ORDER BY views DESC
        LIMIT {limit}
    """).fetchall()
    return [dict(row) for row in res]


def second_query(db):
    """Запрос для расчета суммы, минимума, максимума и среднего значения по числовому полю (rating)."""
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT
            SUM(rating) AS sum_rating,
            MIN(rating) AS min_rating,
            MAX(rating) AS max_rating,
            AVG(rating) AS avg_rating
        FROM bookreviews
    """).fetchone()
    return dict(res)


def third_query(db):
    """Запрос для подсчета частоты встречаемости значений в категориальном поле (genre)."""
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT genre, COUNT(*) AS count
        FROM bookreviews
        GROUP BY genre
        ORDER BY count DESC
    """).fetchall()
    return [dict(row) for row in res]


def fourth_query(db, limit, year_filter):
    """Запрос для фильтрации и сортировки строк."""
    cursor = db.cursor()
    res = cursor.execute(f"""
        SELECT * 
        FROM bookreviews
        WHERE published_year < {year_filter}
        ORDER BY views DESC
        LIMIT {limit}
    """).fetchall()
    return [dict(row) for row in res]


def main():
    # Параметры
    input_csv = "item.csv"
    db_file = "bookreviews.db"
    json_output1 = "query1_result.json"
    json_output4 = "query4_result.json"
    VAR = 5
    limit = VAR + 10

    # Подготовка базы данных
    db = sqlite3.connect(db_file)
    db.row_factory = sqlite3.Row

    # Загрузка данных
    items = load_csv(input_csv)

    # Создание таблицы и вставка данных
    create_table(db)
    insert_data(db, items)

    # Выполнение запросов
    result1 = first_query(db, limit)
    result2 = second_query(db)
    result3 = third_query(db)
    result4 = fourth_query(db, limit, 1933)

    # Сохранение результатов
    with open(json_output1, 'w', encoding='utf-8') as f:
        json.dump(result1, f, ensure_ascii=False, indent=4)

    with open(json_output4, 'w', encoding='utf-8') as f:
        json.dump(result4, f, ensure_ascii=False, indent=4)

    # Вывод результатов
    print("Результат 1:", result1[:3])
    print("Результат 2:", result2)
    print("Результат 3:", result3[:3])
    print("Результат 4:", result4[:3])


if __name__ == "__main__":
    main()