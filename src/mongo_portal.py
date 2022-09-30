from pymongo.server_api import ServerApi
from pymongo import MongoClient
import pandas
from decouple import config

user = config("MONGO_USER")
password = config("MONGO_PASSW")
con = config("MONGO_CON")

db = "test"
collection = "roll"


def connect(user=user, passw=password):
    client = MongoClient(f"mongodb+srv://{user}:{passw}@{con}",
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


if __name__ == '__main__':
    list = [{"a": 1}, {"b": 2}, {"c": "3"}]
    mongo_insert_many(list=list, col="reTest")
    print(mongo_find_all())
    pass
