import json
from pprint import pprint
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from models import create_tables, Publisher, Shop, Book, Stock, Sale


DSN = 'postgresql://postgres:1@localhost:5432/postgres'
engine = sqlalchemy.create_engine(DSN)
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



inp = input('Введите Имя издателя: ')
q = session.query(Book, Publisher).join(Publisher).filter(Publisher.name == inp)
for s in q.all():
    print(s.Publisher.name, s.Book.title)


# q = session.query(Publisher).filter(Publisher.id == input("Введите идентификатор (id) издателя "))
# for s in q.all():
#     print(s.id, s.name)
#
#
# subq = session.query(Shop).all()
# for s in subq:
#     print(s.id, s.name)