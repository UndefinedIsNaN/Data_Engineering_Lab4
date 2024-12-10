import msgpack
import sqlite3
import json

# 1. Чтение данных из файла MsgPack
def read_msgpack(file_path):
    with open(file_path, 'rb') as f:
        return msgpack.unpack(f, raw=False)  # raw=False для преобразования байтов в строки

# 2. Создание таблицы в базе данных SQLite
def create_table(cursor):
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS tournaments (
        id INTEGER PRIMARY KEY,
        name TEXT,
        city TEXT,
        begin TEXT,
        min_rating INTEGER,
        system TEXT,
        time_on_game INTEGER,
        tours_count INTEGER
    );
    ''')

# 3. Запись данных в SQLite
def insert_data(cursor, data):
    for item in data:
        cursor.execute('''
        INSERT INTO tournaments (id, name, city, begin, min_rating, system, time_on_game, tours_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            item['id'], item['name'], item['city'], item['begin'], 
            item['min_rating'], item['system'], item['time_on_game'], item['tours_count']
        ))

# 4. Запросы и их обработка
def get_sorted_rows(cursor, var):
    cursor.execute("SELECT * FROM tournaments ORDER BY min_rating LIMIT ?", (var + 10,))
    return cursor.fetchall()

def get_numeric_stats(cursor):
    cursor.execute("SELECT SUM(min_rating), MIN(min_rating), MAX(min_rating), AVG(min_rating) FROM tournaments")
    return cursor.fetchone()

def get_category_frequency(cursor):
    cursor.execute("SELECT city, COUNT(*) FROM tournaments GROUP BY city")
    return cursor.fetchall()

def get_filtered_rows(cursor, var, predicate):
    cursor.execute(f"SELECT * FROM tournaments WHERE {predicate} ORDER BY min_rating LIMIT ?", (var + 10,))
    return cursor.fetchall()

# 5. Сохранение данных в JSON
def save_to_json(data, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

# Главная функция
def main():
    # Пути к файлу
    input_file = 'item.msgpack'  # Путь к вашему файлу msgpack
    output_json_file = 'sorted_output.json'
    filtered_json_file = 'filtered_output.json'

    # Чтение данных из msgpack
    data = read_msgpack(input_file)
    print(data)

    # Соединение с базой данных SQLite
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()

    # Создание таблицы
    create_table(cursor)

    # Вставка данных
    insert_data(cursor, data)
    conn.commit()

    # Пример переменной VAR
    VAR = 5  # Например, VAR = 5

    # 1. Вывод первых (VAR+10) отсортированных строк в JSON
    sorted_rows = get_sorted_rows(cursor, VAR)
    save_to_json(sorted_rows, output_json_file)

    # 2. Статистика по числовому полю
    stats = get_numeric_stats(cursor)
    print(f"Sum: {stats[0]}, Min: {stats[1]}, Max: {stats[2]}, Average: {stats[3]}")

    # 3. Частота встречаемости для категориального поля
    category_freq = get_category_frequency(cursor)
    print("Category frequencies:")
    for city, count in category_freq:
        print(f"{city}: {count}")

    # 4. Вывод первых (VAR+10) отфильтрованных строк в JSON
    predicate = "time_on_game > 50"  # Например, фильтрация по условию
    filtered_rows = get_filtered_rows(cursor, VAR, predicate)
    save_to_json(filtered_rows, filtered_json_file)

    # Закрытие соединения
    conn.close()

if __name__ == '__main__':
    main()
