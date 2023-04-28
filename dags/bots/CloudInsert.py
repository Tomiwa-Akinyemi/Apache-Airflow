import subprocess 

print("About to call sub process to install pymongo")
def install_package(package_name):
    """
    Installs a package using pip
    Args:
    package_name (str): Name of the package to install.
    """
    subprocess.call(['pip', 'install', package_name])
install_package('pymongo')

print("done installing pymongo")

import pymongo

def get_connection():
    print("About to get connection")
    db_name = None
    try:    
        connection_url=pymongo.MongoClient(f"mongodb+srv://dap:dap@cluster0.dcyzoyt.mongodb.net/test")
        db_name=connection_url.CrimeDataset
        print(db_name)
    except Exception as exc:
        print(exc)
    return db_name

def insert_single_doc(projection=None):
    collection_data = None
    collection_name = "ChicagoCrime1"
    query = {"ID":7397342,"Case Number":"YW352891","Date":"08/10/2022 16:00","Primary Type":"RAPE","Latitude":41.78033068,"Longitude":-87.68489178,"Location":"(41.780330681, -87.684891779)"}
    try:
        db_name = get_connection()
        collection_name=db_name[collection_name]
        print(collection_name)
        collection_data = collection_name.insert_one(query,projection)
    except Exception as exc:
        print(exc)


if __name__ == "__main__":
    insert_single_doc()
