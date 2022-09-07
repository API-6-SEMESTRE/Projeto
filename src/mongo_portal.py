from pymongo.server_api import ServerApi
from pymongo import MongoClient
import pandas

db = "test"
collection = "rock"


def connect(user="cate", passw="api6SEM."):
    client = MongoClient(f"mongodb+srv://{user}:{passw}@cate.rem7mj8.mongodb.net/?retryWrites=true&w=majority",
                         server_api=ServerApi('1'))
    return client


# insere apenas 1 dict
def mongo_insert_one(dict, user="", passw="", db=db, col=collection):
    if (len(user) and len(passw)) <= 3:
        client = connect()
    else:
        client = connect(user, passw)
    db = client[db]
    collection = db[col]
    collection.insert_one(dict)
    client.close()


# insere uma lista de dicts
def mongo_insert_many(list, user="", passw="", db=db, col=collection):
    if (len(user) and len(passw)) <= 3:
        client = connect()
    else:
        client = connect(user, passw)
    db = client[db]
    collection = db[col]
    collection.insert_many(list)


# n concluido
def mongo_find_one(user="", passw="", db=db, col=collection):
    if (len(user) and len(passw)) <= 3:
        client = connect()
    else:
        client = connect(user, passw)
    db = client[db]
    collection = db[col]
    collection.find_one()

# retorna uma lista com todos os docs
def mongo_find_all(user="", passw="", db=db, col=collection):
    if (len(user) and len(passw)) <= 3:
        client = connect()
    else:
        client = connect(user, passw)
    db = client[db]
    collection = db[col]
    search = collection.find()
    data = []
    for i in search:
        data.append(i)
    client.close()
    return data

# TODO pesquisa por data

if __name__ == '__main__':
    #df = pandas.read_excel("Dom Rock/amil_repasse_bronze.xlsx")
    #list = [{"a": 1}, {"b": 2}, {"c": "3"}]
    #mongo_insert_many(list=df, col="repasse")
    #print(mongo_find_all())
    pass