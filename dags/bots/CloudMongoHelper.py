import pymongo

connection_url=pymongo.MongoClient(f"mongodb+srv://dap:dap@cluster0.dcyzoyt.mongodb.net/test")
db_name=connection_url.CrimeDataset
collection = db_name["ChicagoCrime1"]

print("Connected to the database")

execute_query = collection.find({})
print("Done running the query")
for index in execute_query:
    print(index)

