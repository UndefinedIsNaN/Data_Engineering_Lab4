Описание предметной области: 
Датасет содержит информацию о пиве, пивоварнях, которые его производят, и отзывах пользователей на разное пиво. Датасет позволяет анализировать оценки пива по различным параметрам (вкус, аромат, внешний вид), группировать данные по стилям пива и пивоварням, а также выявлять тренды и предпочтения пользователей.

Описание таблиц:

Breweries: Содержит информацию о пивоварнях
  brewery_id (INTEGER, PRIMARY KEY): ИД пивоварни
  brewery_name (TEXT): Название

Beers: Содержит информацию о сортах пива
  beer_beerid (INTEGER, PRIMARY KEY): ИД пива
  beer_name (TEXT): Название пива
  beer_style (TEXT): Сорт пива
  beer_abv (REAL): Процент алкоголя
  brewery_id (INTEGER): ИД пивоварни

Reviews: Содержит информацию об отзывах пользователей на пиво
  review_id (INTEGER, PRIMARY KEY AUTOINCREMENT): ИД отзыва
  beer_beerid (INTEGER, FOREIGN KEY referencing Beers): ИД пива
  review_time (INTEGER): Время публикации отзыва (Unix timestamp но хранится простоо в инте)
  review_profilename (TEXT): Ник ревьюера
  review_overall (REAL): Общая оценка
  review_aroma (REAL): Оценка аромата
  review_appearance (REAL): Оценка внешнего вида
  review_palate (REAL): Оценка послевкусия
  review_taste (REAL): Оценка вкуса