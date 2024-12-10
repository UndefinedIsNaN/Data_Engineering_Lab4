import msgpack
import sqlite3

# 1. Функция для чтения MsgPack
def read_msgpack(file_path):
    with open(file_path, 'rb') as f:
        return msgpack.unpack(f, raw=False)

# 2. Создание второй таблицы
def create_participants_table(cursor):
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS participants (
        id INTEGER,
        participant_name TEXT,
        rating INTEGER,
        result TEXT,
        FOREIGN KEY (id) REFERENCES tournaments(id)
    );
    ''')

# 3. Вставка данных во вторую таблицу
def insert_participants_data(cursor, data):
    for item in data:
        cursor.execute('''
        INSERT INTO participants (id, participant_name, rating, result)
        VALUES (?, ?, ?, ?)
        ''', (item['id'], item['participant_name'], item['rating'], item['result']))

# 4. Запросы с использованием связи между таблицами
def get_tournaments_with_participants(cursor):
    cursor.execute('''
    SELECT t.name, t.city, p.participant_name, p.rating, p.result
    FROM tournaments t
    JOIN participants p ON t.id = p.id
    ''')
    return cursor.fetchall()

def get_participant_count_by_tournament(cursor):
    cursor.execute('''
    SELECT t.name, COUNT(p.participant_name) as participant_count
    FROM tournaments t
    JOIN participants p ON t.id = p.id
    GROUP BY t.name
    ''')
    return cursor.fetchall()

def get_high_rating_participants(cursor, min_rating):
    cursor.execute('''
    SELECT t.name, p.participant_name, p.rating
    FROM tournaments t
    JOIN participants p ON t.id = p.id
    WHERE p.rating > ?
    ORDER BY p.rating DESC
    ''', (min_rating,))
    return cursor.fetchall()

# Главная функция
def main():
    # Пути к файлам
    second_file = 'subitem.msgpack'

    # Чтение данных из второго файла
    participants_data = read_msgpack(second_file)
    print(participants_data)

    # Подключение к базе данных
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()

    # Создание второй таблицы
    create_participants_table(cursor)

    # Вставка данных во вторую таблицу
    insert_participants_data(cursor, participants_data)
    conn.commit()

    # Выполнение запросов
    print("Турниры и участники:")
    tournaments_with_participants = get_tournaments_with_participants(cursor)
    for row in tournaments_with_participants:
        print(row)

    print("\nКоличество участников по турнирам:")
    participant_count = get_participant_count_by_tournament(cursor)
    for row in participant_count:
        print(row)

    print("\nУчастники с рейтингом выше 2250:")
    high_rating_participants = get_high_rating_participants(cursor, 2250)
    for row in high_rating_participants:
        print(row)

    # Закрытие соединения
    conn.close()

if __name__ == '__main__':
    main()
