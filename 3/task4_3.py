import pandas as pd
import msgpack
from sqlalchemy import create_engine

def load_and_process_files(json_path, pkl_path):
    """Загружает и обрабатывает файлы JSON и PKL, возвращает объединенный DataFrame."""
    # Загрузка JSON-файла
    with open('C:/Users/Mitya/Downloads/3/_part_2.msgpack', 'rb') as file:
        json_data = pd.DataFrame(msgpack.load(file))

    # Загрузка PKL-файла
    pkl_data = pd.read_csv('C:/Users/Mitya/Downloads/3/_part_1.csv',delimiter=';')

    # Если PKL содержит список, преобразуем его в DataFrame
    if isinstance(pkl_data, list):
        pkl_data = pd.DataFrame(pkl_data)

    # Преобразование типов данных в PKL
    pkl_data['duration_ms'] = pd.to_numeric(pkl_data['duration_ms'], errors='coerce')
    pkl_data['year'] = pd.to_numeric(pkl_data['year'], errors='coerce')
    pkl_data['tempo'] = pd.to_numeric(pkl_data['tempo'], errors='coerce')

    # Определяем общие столбцы
    common_columns = list(set(json_data.columns).intersection(set(pkl_data.columns)))
    print("\nОбщие столбцы:", common_columns)

    # Объединяем данные
    combined_data = pd.concat([json_data[common_columns], pkl_data[common_columns]])

    # Сохраняем объединенные данные для проверки
    combined_data.to_csv('third_task_combined_data.csv', index=False)
    print("\nОбъединенные данные сохранены в 'combined_data.csv'.")

    return combined_data

def save_query_result_to_file(query, connection, output_file, as_json=False):
    """Выполняет запрос к базе данных и сохраняет результат в файл."""
    result = pd.read_sql(query, connection)
    if as_json:
        result.to_json(output_file, orient='records')
    else:
        result.to_csv(output_file, index=False)
    print(f"\nРезультат сохранен в '{output_file}'.")
    return result

def main(json_path, pkl_path):
    """Основная функция выполнения программы."""
    # Шаг 1: Загрузка и объединение данных
    combined_data = load_and_process_files(json_path, pkl_path)

    # Шаг 2: Создание базы данных
    engine = create_engine('sqlite:///third_task_music_data.db')
    table_name = 'music'
    combined_data.to_sql(table_name, engine, if_exists='replace', index=False)
    connection = engine.connect()

    # VAR для запросов
    VAR = 5

    # Шаг 3: Выполнение запросов

    # Запрос 1: Первые (VAR+10) строк, отсортированных по популярности
    query_1 = f"SELECT * FROM {table_name} ORDER BY tempo DESC LIMIT {VAR + 10}"
    save_query_result_to_file(query_1, connection, 'third_task_VAR+10_sorted_tempo.json', as_json=True)

    # Запрос 2: Сумма, минимум, максимум, среднее для популярности
    query_2 = f"""
    SELECT 
        SUM(tempo) AS total_tempo,
        MIN(tempo) AS min_tempo,
        MAX(tempo) AS max_tempo,
        AVG(tempo) AS avg_tempo
    FROM {table_name}
    """
    save_query_result_to_file(query_2, connection, 'third_task_tempo_stats.csv')

    # Запрос 3: Частота встречаемости жанров
    query_3 = f"""
    SELECT genre, COUNT(*) AS frequency
    FROM {table_name}
    GROUP BY genre
    ORDER BY frequency DESC
    """
    save_query_result_to_file(query_3, connection, 'third_task_genre_frequencies.csv')

    # Запрос 4: Первые (VAR+15) строки, отфильтрованные по году > 2010
    query_4 = f"""
    SELECT * FROM {table_name} 
    WHERE year > 2010 
    ORDER BY tempo DESC 
    LIMIT {VAR + 15}
    """
    save_query_result_to_file(query_4, connection, './third_task_VAR+15_sorted_year_more_2010.json', as_json=True)

    print("\nВсе запросы выполнены и результаты сохранены.")

main('C:/Users/Mitya/Downloads/3/_part_2.json', 'C:/Users/Mitya/Downloads/3/_part_1.pkl')

