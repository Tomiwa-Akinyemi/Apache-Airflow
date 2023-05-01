# Now we have the data stored in a table on local postgre..
# In this class we will then fetch the data and do some checks for missing data, we then create a final table with the important columns and random 10k rows for easier fetching and analysis.
# Random rows cause it means everytime we run this pipeline we will have random data saved into the table and a dynamic visualization

from SaveDataToPostgre import checkIfTableExists
from SaveDataToPostgre import dropTable

import psycopg2
import psycopg2.extras as extras
import pandas as pd
import pandas.io.sql as sqlio


selectQuery = """
SELECT CaseNumber AS CaseNumber, Date AS Date, Block AS Block,
IUCR AS Iucr, PrimaryType AS PrimaryType, Description AS Description, LocationDescription AS LocationDescription,
Arrest AS Arrest, Domestic AS Domestic, Beat AS Beat, District AS District, Ward AS Ward, CommunityArea AS CommunityArea,
FbiCode AS FBICode, XCoordinate AS XCoordinate, YCoordinate AS YCoordinate, Year AS Year, UpdatedOn AS UpdatedOn,
Latitude AS Latitude, Longitude AS Longitude, Location AS Location FROM ChicagoCrime_Staging
"""

def fetchDataFromDB():
    df = None
    try:
        dbConnection = psycopg2.connect(
            user = "airflow",
            password = "airflow",
            host = "postgres",
            database = "ChicagoCrime")
        df = sqlio.read_sql_query(selectQuery, dbConnection)
        print("Fetched data successfully..")
    except (Exception , psycopg2.Error) as dbError :
        print ("Error while retrieving data from PostgreSQL", dbError)
    finally:        
        if dbConnection in locals():
            dbConnection.close()
        return df

def preProcessData():
    retrievedData = fetchDataFromDB()
    #now we remove columns and drop duplicates on the case number column
    retrievedData.drop_duplicates(subset="casenumber",keep='first', inplace=True)
    #drop unneccesary columns
    retrievedData.drop(['updatedon', 'location', 'locationdescription', 'beat', 'fbicode', 'year', 'ward', 'district', 'xcoordinate', 'ycoordinate'], axis=1, inplace=True)
    return retrievedData

createTableQuery = """
CREATE TABLE ChicagoCrime_Final (
	Incident_Id serial PRIMARY KEY,
	CaseNumber VARCHAR (17),
	Date TIMESTAMP,
	Block VARCHAR(50),
	IUCR VARCHAR(10),
	PrimaryType VARCHAR ( 50 ),
	Description VARCHAR ( 500 ),
	Arrest BOOLEAN,
	Domestic BOOLEAN,
	CommunityArea VARCHAR(10),
	Latitude VARCHAR(50),
	Longitude VARCHAR(50)
);
"""


def createTable():
    if(checkIfTableExists("ChicagoCrime_Final")):
        #drop table
        dropTable("ChicagoCrime_Final")
    try:
        dbConnection = psycopg2.connect(
            user = "airflow",
            password = "airflow",
            host = "postgres",
            database = "ChicagoCrime")
        dbConnection.set_isolation_level(0) # AUTOCOMMIT
        dbCursor = dbConnection.cursor()
        dbCursor.execute(createTableQuery)
        dbCursor.close()
        print("Table created")
    except (Exception , psycopg2.Error) as dbError :
        print ("Error while creating table on PostgreSQL", dbError)
    finally:
        if dbConnection in locals(): 
            dbConnection.close()


def insertDataToNewTable():
    data = preProcessData()
    print("Retrieved processed data")
    tuples = [tuple(x) for x in data.to_numpy()]
    cols = ','.join(list(data.columns.str.replace(" ","")))
    createTable()
    insertQuery = "INSERT INTO %s(%s) VALUES %%s" % ("ChicagoCrime_Final", cols)
    try:
        dbConnection = psycopg2.connect(
            user = "airflow",
            password = "airflow",
            host = "postgres",
            database = "ChicagoCrime")
        dbConnection.set_isolation_level(0) # AUTOCOMMIT
        dbCursor = dbConnection.cursor()
        extras.execute_values(dbCursor, insertQuery, tuples)
        dbCursor.close()
        print("Data insertion completed")
    except (Exception , psycopg2.Error) as dbError :
        dbConnection.rollback()
        print ("Error while loading data into PostgreSQL", dbError)
    finally:
        if dbConnection in locals(): 
            dbConnection.close()

if __name__ == "__main__":
    insertDataToNewTable()
