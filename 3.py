import csv
import pickle
import sqlite3
import json
import os


def read_csv(file):
    """Чтение данных из CSV файла."""
    items = []
    with open(file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            # Преобразование данных
            row['duration_ms'] = int(row['duration_ms'])
            row['year'] = int(row['year'])
            row['tempo'] = float(row['tempo'])
            row['energy'] = float(row['energy'])
            items.append(row)
    return items


def read_pkl(file):
    """Чтение данных из PKL файла."""
    with open(file, "rb") as f:
        items = pickle.load(f)
        for item in items:
            # Преобразование данных
            item['duration_ms'] = int(item['duration_ms'])
            item['year'] = int(item['year'])
            item['tempo'] = float(item['tempo'])
            item['energy'] = float(item['energy'])
    return items


def create_table(db):
    """Создает таблицу для объединенных данных."""
    cursor = db.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS music_data (
            artist TEXT,
            song TEXT,
            duration_ms INTEGER,
            year INTEGER,
            tempo REAL,
            genre TEXT,
            energy REAL
        )
    """)
    db.commit()


def insert_data(db, items):
    """Вставляет данные в таблицу."""
    cursor = db.cursor()
    cursor.executemany("""
        INSERT INTO music_data (artist, song, duration_ms, year, tempo, genre, energy)
        VALUES (:artist, :song, :duration_ms, :year, :tempo, :genre, :energy)
    """, items)
    db.commit()


def first_query(db, limit):
    """Вывод первых (VAR+10) строк, отсортированных по energy."""
    cursor = db.cursor()
    res = cursor.execute(f"""
        SELECT * FROM music_data
        ORDER BY energy DESC
        LIMIT {limit}
    """)
    return [dict(row) for row in res]


def second_query(db):
    """Анализ числового поля (сумма, мин, макс, среднее по duration_ms)."""
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT 
            SUM(duration_ms) AS total_duration,
            MIN(duration_ms) AS min_duration,
            MAX(duration_ms) AS max_duration,
            AVG(duration_ms) AS avg_duration
        FROM music_data
    """)
    return dict(res.fetchone())


def third_query(db):
    """Частота встречаемости для поля genre."""
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT genre, COUNT(*) AS count
        FROM music_data
        GROUP BY genre
        ORDER BY count DESC
    """)
    return [dict(row) for row in res]


def fourth_query(db, limit):
    """Вывод первых (VAR+15) строк, отфильтрованных по предикату (year > 2000)."""
    cursor = db.cursor()
    res = cursor.execute(f"""
        SELECT * FROM music_data
        WHERE year > 2000
        ORDER BY tempo ASC
        LIMIT {limit}
    """)
    return [dict(row) for row in res]


def main():
    # Файлы
    csv_file = "_part_1.csv"
    pkl_file = "_part_2.pkl"
    db_file = "music_data.db"
    json_output1 = "query1_result.json"
    json_output2 = "query2_result.json"
    json_output3 = "query3_result.json"
    json_output4 = "query4_result.json"

    # Проверяем существование файлов
    if not os.path.exists(csv_file) or not os.path.exists(pkl_file):
        print("CSV или PKL файл не найден. Проверьте пути.")
        return

    # Подготовка базы данных
    db = sqlite3.connect(db_file)
    db.row_factory = sqlite3.Row

    # Чтение данных из файлов
    csv_data = read_csv(csv_file)
    pkl_data = read_pkl(pkl_file)

    # Создание таблицы и вставка данных
    create_table(db)
    insert_data(db, csv_data + pkl_data)

    # Выполнение запросов
    result1 = first_query(db, limit=15)  # VAR + 10
    result2 = second_query(db)
    result3 = third_query(db)
    result4 = fourth_query(db, limit=20)  # VAR + 15

    # Сохранение результатов в файлы JSON
    with open(json_output1, 'w', encoding='utf-8') as f:
        json.dump(result1, f, ensure_ascii=False, indent=4)

    with open(json_output2, 'w', encoding='utf-8') as f:
        json.dump(result2, f, ensure_ascii=False, indent=4)

    with open(json_output3, 'w', encoding='utf-8') as f:
        json.dump(result3, f, ensure_ascii=False, indent=4)

    with open(json_output4, 'w', encoding='utf-8') as f:
        json.dump(result4, f, ensure_ascii=False, indent=4)

    # Вывод результатов в консоль
    print("Результат 1 (первые строки):", result1[:3])
    print("Результат 2 (анализ числового поля):", result2)
    print("Результат 3 (частота жанров):", result3[:3])
    print("Результат 4 (фильтр по году):", result4[:3])


if __name__ == "__main__":
    main()
