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


# get json data from 'json/files.json' and convert to variables
with open('json/header.json') as fileHeader:
    header = json.load(fileHeader)

with open('json/monthlyPay.json') as fileMonthlyPay:
    monthlyPay = json.load(fileMonthlyPay)

with open('json/transfer.json') as fileTransfer:
    transfer = json.load(fileTransfer)

# insert data in MongoDB collections
'''
mongo_insert_many(list=header, col="header")
mongo_insert_many(list=monthlyPay, col="monthlyPay")
mongo_insert_many(list=transfer, col="transfer")
'''

print('header: \n', mongo_find_all(col='header'))
print('\n\n\nmonthlyPay: \n', mongo_find_all(col='monthlyPay'))
print('\n\n\ntransfer: \n', mongo_find_all(col='transfer'))
