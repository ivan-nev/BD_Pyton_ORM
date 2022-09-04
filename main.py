import json
from pprint import pprint
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from models import create_tables, Publisher, Shop, Book, Stock, Sale

user_name = 'postgres'
password = '1'
host = 'localhost'
port = '5432'
bd_name = 'postgres'
DSN = f'postgresql://{user_name}:{password}@{host}:{port}/{bd_name}'
engine = sqlalchemy.create_engine(DSN)
con = engine.connect()
create_tables(engine)

Session = sessionmaker(bind=engine)
session = Session()

with open('tests_data.json', 'r') as fd:
    data = json.load(fd)

for record in data:
    model = {
        'publisher': Publisher,
        'shop': Shop,
        'book': Book,
        'stock': Stock,
        'sale': Sale,
    }[record.get('model')]
    session.add(model(id=record.get('pk'), **record.get('fields')))
session.commit()

# inp = input('Введите Имя издателя: ')
inp = 'O’Reilly'


print('вариант1')
print()
subq = session.query(Shop.name, Stock.id_book).join(Stock.stock_shops).subquery()
subq2 = session.query(Book, subq).join(subq, Book.id == subq.c.id_book).subquery()
subq3 = session.query(Publisher.name, subq2).join(subq2, Publisher.id == subq2.c.id_publisher).filter(Publisher.name == inp)
print (subq3)
for s in subq3:
    print(s)

print('вариант2')
print()
subq = session.query(Shop, Stock).join(Stock.stock_shops).subquery()
subq2 = session.query(Book, subq).join(subq, Book.id == subq.c.id_book).subquery()
subq3 = session.query(Publisher, subq2).join(subq2, Publisher.id == subq2.c.id_publisher).filter(Publisher.name == inp)
print (subq3)
for s in subq3:
    print(s.Publisher.name)

print()
print('вар 3')
q = (con.execute(
'''
select s."name" , b.title, p."name"
from shop as s
join stock as st on s.id = st.id_shop
join book as b on st.id_book = b.id
join publisher as p on b.id_publisher = p.id
where p."name" = %s
order  by s."name"
''',(inp,)
).fetchall())
for s in q:
    print(s)



