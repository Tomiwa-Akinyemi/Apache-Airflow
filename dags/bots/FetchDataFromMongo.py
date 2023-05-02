import pymongo
import pandas

connection_url=pymongo.MongoClient(f"mongodb+srv://dap:dap@cluster0.dcyzoyt.mongodb.net/test", maxPoolSize=50, maxIdleTimeMS=10000, waitQueueTimeoutMS=10000, connectTimeoutMS=20000)
db_name=connection_url.CrimeDataset
collection_name = "ChicagoCrime"
# connection_url=pymongo.MongoClient(f"mongodb://dap:dap@localhost:27017")
# db_name=connection_url.ChicagoCrime
# collection_name = "Crime"
results = None

# Collection Validation: check if collection exist
try:
    db_name.validate_collection(collection_name) 
    print("Collection validated, continue operation...")
    collection = db_name[collection_name]
    print("Connected to the database")
    results = collection.find({},{ "_id": 0,"ï»¿ID":0})#.limit(20)
    print("Done running the query.")
    df = pandas.DataFrame(list(results))
    # print(df.head())
except pymongo.errors.OperationFailure as exc:  # If the collection doesn't exist
    print("This collection doesn't exist")
    print(exc)

def getMongoData():
    return df


