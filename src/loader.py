from pymongo.server_api import ServerApi
from pymongo import MongoClient
import json

db = "test"
collection = "rock"


def connect(user="cate", passw="api6SEM."):
    client = MongoClient(f"mongodb+srv://{user}:{passw}@cate.rem7mj8.mongodb.net/?retryWrites=true&w=majority",
                         server_api=ServerApi('1'))
    return client


def mongo_insert_many(list, user="", passw="", db=db, col=collection):
    if (len(user) and len(passw)) <= 3:
        client = connect()
    else:
        client = connect(user, passw)
    db = client[db]
    collection = db[col]
    collection.insert_many(list)


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


def main():
   # get json data from 'json/files.json' and convert to variables
    with open('json/header.json', encoding='utf-8') as file_header:
        header = json.load(file_header)

    with open('json/monthly_pay.json', encoding='utf-8') as file_monthly:
        monthly_pay = json.load(file_monthly)

    with open('json/transfer.json', encoding='utf-8') as file_transfer:
        transfer = json.load(file_transfer)

    # insert data in MongoDB collections
    mongo_insert_many(list=header, col="header")
    mongo_insert_many(list=monthly_pay, col="monthly_pay")
    mongo_insert_many(list=transfer, col="transfer")


if __name__ == '__main__':
    main()
