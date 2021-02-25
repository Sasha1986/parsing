import pymongo

db_client = pymongo.MongoClient("mongodb://localhost:27017")
db = db_client["gb_data_miner_16_02_2021"]
collection = db["magnit"]

for item in collection.find():
    print(item)