import pymongo
import pandas

#connection_url=pymongo.MongoClient(f"mongodb+srv://dap:dap@cluster0.dcyzoyt.mongodb.net/test")
connection_url=pymongo.MongoClient(f"mongodb://dap:dap@localhost:27017")
#db_name=connection_url.CrimeDataset
db_name=connection_url.ChicagoCrime
collection_name = "Crime"
results = None

# Collection Validation: check if collection exist
try:
    db_name.validate_collection(collection_name) 
    print("Collection validated, continue operation...")
    collection = db_name[collection_name]
    print("Connected to the database")
    results = collection.find({},{ "_id": 0,"ï»¿ID":0}).limit(20)
    print("Done running the query.")
    df = pandas.DataFrame(list(results))
    #print(df.head())
except pymongo.errors.OperationFailure:  # If the collection doesn't exist
    print("This collection doesn't exist")

def getMongoData():
    return df


