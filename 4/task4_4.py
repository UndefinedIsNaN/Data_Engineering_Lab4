import sqlite3
import msgpack
import csv

def connect_to_db(filename):
    conn = sqlite3.connect(filename)
    conn.row_factory = sqlite3.Row
    return conn

def create_table(db):
    cur = db.cursor()
    query = '''
        CREATE TABLE IF NOT EXISTS products(
            id integer primary key,
            name text,
            price float,
            quantity integer,
            category text,
            fromCity text,
            isAvailable bool,
            views integer,
            version integer default 0)
    '''
    cur.execute(query)

def insert_into_db(db, data):
    cur = db.cursor()
    query = '''
            INSERT INTO products (name, price, quantity,
                              category, fromCity, isAvailable, views)
            VALUES (:name, :price, :quantity,
                              :category, :fromCity, :isAvailable, :views)
            '''
    cur.executemany(query, data)
    db.commit()

def update_db(db, updates):

    for product in updates:
        cur = db.cursor()
        name = product['name']
        param = product['param']

        if product['method'] == 'remove':
            cur.execute('DELETE FROM products WHERE name = ?', [name])

        elif product['method'] == 'price_percent':
            cur.execute('''UPDATE products
                           SET price = ROUND(price * ( 1 + ?), 2),
                               version= version + 1
                           WHERE name = ?''', [float(param), name])

        elif product['method'] == 'price_abs':
            cur.execute('''UPDATE products
                           SET price = price + ?,
                               version= version + 1
                           WHERE name = ?''', [float(param), name])

        elif product['method'] in ['quantity_add', 'quantity_sub']:
            cur.execute('''UPDATE products
                           SET quantity = quantity + ?,
                               version= version + 1
                           WHERE name = ?''', [float(param), name])

        elif product['method'] == 'available':
            cur.execute('''UPDATE products
                           SET isAvailable = ?,
                               version= version + 1
                           WHERE name = ?''', [param == 'True', name])

        db.commit()

def select_from_db(db, query):
    cur = db.cursor()
    res = cur.execute(query)
    return [dict(row) for row in res.fetchall()]

def read_csv(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file,
                                ['name', 'method', 'param'],
                               delimiter=';')
        reader.__next__()
        items = list(reader)
        return items

def read_msgpack(filename):
    with open('C:/Users/Mitya/Downloads/4/_product_data.msgpack', 'rb') as file:
        data = msgpack.load(file)

    # проверка на полноту данных
    items = []
    for item in data:
        if len(item.keys()) == 7:
            items.append(item)
    return items

# reading main file
data_main = read_msgpack('C:/Users/Mitya/Downloads/4/_product_data.msgpack')

# reading file with updates
data_upd = read_csv('C:/Users/Mitya/Downloads/4/_update_data.csv')

# connect to db
db = connect_to_db('C:/Users/Mitya/Downloads/4/fourth_task1.db')

# create table (if not exists)
create_table(db)

# load main data into table
insert_into_db(db, data_main)

# update db
update_db(db, data_upd)

# вывести топ-10 самых обновляемых товаров
query = 'SELECT * FROM products ORDER BY version DESC LIMIT 10'
res = select_from_db(db, query)
for el in res:
    print(el['name'] + ' из ' + el['fromCity'])

# проанализировать цены товаров, найдя (сумму, мин, макс, среднее) для каждой группы,
# а также количество товаров в группе
query = '''
        SELECT
            category,
            ROUND(SUM(price), 2) as sum,
            ROUND(AVG(price), 2) as avg,
            ROUND(MIN(price), 2) as min,
            ROUND(MAX(price), 2) as max,
            COUNT(price) as count
        FROM products
        GROUP BY category
        '''
print(select_from_db(db, query))

# проанализировать остатки товаров, найдя (сумму, мин, макс, среднее) для каждой группы товаров
query = '''
        SELECT
            category,
            ROUND(SUM(quantity), 2) as sum,
            ROUND(AVG(quantity), 2) as avg,
            ROUND(MIN(quantity), 2) as min,
            ROUND(MAX(quantity), 2) as max,
            COUNT(quantity) as count
        FROM products
        GROUP BY category
        '''
print(select_from_db(db, query))

# 10 наиболее просматриваемых товаров из категории 'fruit'
query = '''
SELECT name
FROM products
WHERE category = 'fruit'
ORDER BY views DESC
LIMIT 10
'''
for el in select_from_db(db, query):
    print(el['name'])

db.close()