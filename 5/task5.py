import csv
import sqlite3
import json

def create_and_populate_db(csv_filepath, json_filepath, db_filepath):
  conn = sqlite3.connect(db_filepath)
  cursor = conn.cursor()
  
  cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name = 'Breweries'")

  if cursor.fetchone():
    print("Таблицы уже существует.")
    return 

  cursor.execute('''
      CREATE TABLE IF NOT EXISTS Breweries (
          brewery_id INTEGER PRIMARY KEY,
          brewery_name TEXT UNIQUE
      )
  ''')
  cursor.execute('''
      CREATE TABLE IF NOT EXISTS Beers (
          beer_beerid INTEGER PRIMARY KEY,
          beer_name TEXT,
          beer_style TEXT,
          beer_abv REAL,
          brewery_id INTEGER NOT NULL
      )
  ''')
  cursor.execute('''
      CREATE TABLE IF NOT EXISTS Reviews (
          review_id INTEGER PRIMARY KEY AUTOINCREMENT,
          beer_beerid INTEGER NOT NULL,
          review_time INTEGER,
          review_profilename TEXT,
          review_overall REAL,
          review_aroma REAL,
          review_appearance REAL,
          review_palate REAL,
          review_taste REAL
      )
  ''')


  # Заполнение из csv
  with open(csv_filepath, "r", encoding="utf-8") as file:
    reader = csv.DictReader(file)
    for row in reader:
        
      cursor.execute('''
                      INSERT OR IGNORE INTO Breweries 
                      (brewery_id, brewery_name) 
                      VALUES (?, ?)
                              ''', (row['brewery_id'], row['brewery_name']))
      
      cursor.execute('''
                      INSERT OR IGNORE INTO Beers 
                      (beer_beerid, beer_name, beer_style, beer_abv, brewery_id) 
                      VALUES (?, ?, ?, ?, ?)
                              ''', (row['beer_beerid'], row['beer_name'], row['beer_style'], row['beer_abv'], row['brewery_id']))
      
      cursor.execute('''
                      INSERT INTO Reviews 
                      (beer_beerid, review_time, review_profilename, review_overall, 
                      review_aroma, review_appearance, review_palate, review_taste) 
                      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                     ''', 
                      (row['beer_beerid'], row['review_time'], row['review_profilename'], row['review_overall'], 
                      row['review_aroma'], row['review_appearance'], row['review_palate'], row['review_taste']))


  # Заполнение из JSON
  try:
    with open(json_filepath, "r", encoding="utf-8") as f:
      json_data = json.load(f)
      for item in json_data:
        cursor.execute('''
                       INSERT OR IGNORE INTO Breweries 
                       (brewery_id, brewery_name) 
                       VALUES (?, ?)
                               ''', (item['brewery_id'], item['brewery_name']))
        
        cursor.execute('''
                       INSERT OR IGNORE INTO Beers 
                       (beer_beerid, beer_name, beer_style, beer_abv, brewery_id) 
                       VALUES (?, ?, ?, ?, ?)
                               ''', (item['beer_beerid'], item['beer_name'], item['beer_style'], item['beer_abv'], item['brewery_id']))
        cursor.execute('''
                       INSERT INTO Reviews 
                       (beer_beerid, review_time, review_profilename, review_overall, 
                       review_aroma, review_appearance, review_palate, review_taste) 
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                       ''',
                         (item['beer_beerid'], item['review_time'], item['review_profilename'], item['review_overall'], 
                          item['review_aroma'], item['review_appearance'], item['review_palate'], item['review_taste']))
        
  except (FileNotFoundError, json.JSONDecodeError) as e:
    print(f"Ошибка при работе с JSON-файлом: {e}")

  conn.commit()
  conn.close()
  print(f"База данных '{db_filepath}' успешно создана и заполнена.")


def json_save(data, json_filepath):
  with open(json_filepath, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=4)

def shrek(db_filepath):
  conn = sqlite3.connect(db_filepath)
  cursor = conn.cursor()

  #запрос 1 тест бд, стащим 50 записей из датасета
  cursor.execute('''
                 SELECT
                   b.brewery_id,
                   b.brewery_name,
                   r.review_time,
                   r.review_overall,
                   r.review_aroma,
                   r.review_appearance,
                   r.review_profilename,
                   be.beer_style,
                   r.review_palate,
                   r.review_taste,
                   be.beer_name,
                   be.beer_abv,
                   be.beer_beerid
                 FROM Reviews AS r
                 JOIN Beers AS be ON r.beer_beerid = be.beer_beerid
                 JOIN Breweries AS b ON be.brewery_id = b.brewery_id
                 LIMIT 50;''')

  reviews = cursor.fetchall()
  column_names = ['brewery_id', 'brewery_name', 'review_time', 'review_overall', 
                  'review_aroma', 'review_appearance', 'review_profilename', 'beer_style',
                  'review_palate', 'review_taste', 'beer_name', 'beer_abv', 'beer_beerid']
  data = [dict(zip(column_names, row)) for row in reviews]
  json_save(data, "task5_output1_test.json")

  #2 Средняя оценка пива каждого стиля
  cursor.execute('''    
                  SELECT beer_style, AVG(review_overall) AS average_rating
                  FROM Beers AS b
                  JOIN Reviews AS r ON b.beer_beerid = r.beer_beerid
                  GROUP BY beer_style
                  ORDER BY average_rating DESC;
                  ''')

  styles_avg = cursor.fetchall()
  column_names = ['beer_style', 'average_rating']
  data = [dict(zip(column_names, row)) for row in styles_avg]
  json_save(data, "task5_output2_styles_avg.json")

  #3 Наименования пивоварен, производящих Английский стаут менее 5ти градусов
  cursor.execute('''    
                  SELECT DISTINCT b.brewery_name, be.beer_name, be.beer_abv
                  FROM Breweries AS b
                  JOIN Beers AS be ON b.brewery_id = be.brewery_id
                  WHERE be.beer_style = 'English Stout'
                    AND be.beer_abv < 5;
                  ''')

  stout_breweries = cursor.fetchall()
  column_names = ['brewery_name', 'beer_name', 'beer_abv']
  data = [dict(zip(column_names, row)) for row in stout_breweries]
  json_save(data, "task5_output3_stout_breweries.json")

  #4 Отзывы пользователя с наибольшим количеством отзывов
  cursor.execute('''    
                  SELECT 
                   be.beer_name, r.review_time, r.review_overall, r.review_aroma,
                   r.review_appearance, r.review_profilename, be.beer_style, r.review_palate, r.review_taste
                  FROM Reviews AS r
                  JOIN Beers AS be ON r.beer_beerid = be.beer_beerid
                  WHERE r.review_profilename in (
                                                    SELECT review_profilename
                                                    FROM Reviews
                                                    GROUP BY review_profilename
                                                    ORDER BY COUNT(*) DESC
                                                    LIMIT 1
                                                )
                  ''')

  productive_reviewer = cursor.fetchall()
  column_names = ['brewery_name', 'review_time', 'review_overall', 'review_aroma', 
                  'review_appearance', 'review_profilename', 'beer_style', 'review_palate', 'review_taste']
  data = [dict(zip(column_names, row)) for row in productive_reviewer]
  json_save(data, "task5_output4_productive_reviewer.json")

  #5 ТОП-15 самых вкуснопахнущих пив(а)
  cursor.execute('''    
                  SELECT be.beer_name, AVG(r.review_aroma) AS average_aroma
                  FROM Beers AS be
                  JOIN Reviews AS r ON be.beer_beerid = r.beer_beerid
                  GROUP BY be.beer_name
                  ORDER BY average_aroma DESC
                  LIMIT 15;
                  ''')

  aromative_beer = cursor.fetchall()
  column_names = ['beer_name', 'average_aroma']
  data = [dict(zip(column_names, row)) for row in aromative_beer]
  json_save(data, "task5_output5_aromative_beer.json")

  #6 Пивоварни, имющие среднюю оценку их пива не выше 4.2
  cursor.execute('''    
                  SELECT b.brewery_name, AVG(r.review_overall) AS avg_brewery_rating
                  FROM Breweries AS b
                  JOIN Beers AS be ON b.brewery_id = be.brewery_id
                  JOIN Reviews AS r ON be.beer_beerid = r.beer_beerid
                  GROUP BY b.brewery_name
                  HAVING AVG(r.review_overall) > 4.2;
                  ''')

  high_level_breweries = cursor.fetchall()
  column_names = ['brewery_name', 'avg_brewery_rating']
  data = [dict(zip(column_names, row)) for row in high_level_breweries]
  json_save(data, "task5_output6_high_level_breweries.json")
  
  conn.close()


csv_filepath = "dataset/beer_reviews.csv" 
json_filepath = "dataset/breweries.json"
db_filepath = "task5_database.db"
create_and_populate_db(csv_filepath, json_filepath, db_filepath)
shrek(db_filepath)
