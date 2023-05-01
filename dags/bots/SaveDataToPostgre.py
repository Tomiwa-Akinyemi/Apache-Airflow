# Easy way is to hardcode the table column and stuff
# Hard way is to get the column from the mongo dataframe and use that to create your table.... 


#basically we will call the other class to return mongo data, extract the columns and create a table using SQL script, then we loop through and save the data into postgre....

from FetchDataFromMongo import getMongoData

import psycopg2
import psycopg2.extras as extras


#'Case Number', 'Date', 'Block', 'IUCR', 'Primary Type', 'Description', 'Location Description', 'Arrest', 'Domestic', 'Beat', 'District', 'Ward', 'Community Area', 'FBI Code', 'X Coordinate', 'Y Coordinate', 'Year', 'Updated On', 'Latitude', 'Longitude', 'Location'

def checkIfTableExists(tableName):
    isExist = True
    print("Checking if table exist")
    try:
        dbConnection = psycopg2.connect(
            user = "airflow",
            password = "airflow",
            host = "postgres",
            database = "ChicagoCrime")
        dbConnection.set_isolation_level(0) # AUTOCOMMIT
        dbCursor = dbConnection.cursor()
        dbCursor.execute("select exists(select * from information_schema.tables where table_name=%s)", (tableName.lower(),))
        isExist = dbCursor.fetchone()[0]
        dbCursor.close()
    except (Exception , psycopg2.Error) as dbError :
        print ("Error while connecting to PostgreSQL", dbError)
    finally:        
        if dbConnection in locals():
            dbConnection.close()
        return isExist

def dropTable(tableName):
    try:
        dbConnection = psycopg2.connect(
            user = "airflow",
            password = "airflow",
            host = "postgres",
            database = "ChicagoCrime")
        dbConnection.set_isolation_level(0) # AUTOCOMMIT
        dbCursor = dbConnection.cursor()
        dbCursor.execute("DROP table IF EXISTS " +tableName.lower())
        print("Table dropped..")
        dbCursor.close()
    except (Exception , psycopg2.Error) as dbError :
        print ("Error while dropping table in PostgreSQL", dbError)
    finally:        
        if dbConnection in locals():
            dbConnection.close()

createTableQuery = """
CREATE TABLE ChicagoCrime_Staging (
	Incident_Id serial PRIMARY KEY,
	CaseNumber VARCHAR (17),
	Date TIMESTAMP,
	Block VARCHAR(50),
	IUCR VARCHAR(10),
	PrimaryType VARCHAR ( 50 ),
	Description VARCHAR ( 500 ),
	LocationDescription VARCHAR ( 500 ),
	Arrest BOOLEAN,
	Domestic BOOLEAN,
	Beat VARCHAR ( 50 ),
	District VARCHAR ( 50 ),
	Ward VARCHAR ( 10 ),
	CommunityArea VARCHAR(10),
 	FBICode VARCHAR(5),
 	XCoordinate VARCHAR ( 50 ),
 	YCoordinate VARCHAR ( 50 ),
	Year VARCHAR (4),
	UpdatedOn TIMESTAMP,
	Latitude VARCHAR(50),
	Longitude VARCHAR(50),
	Location VARCHAR(100)
);
"""

def createTable():
    if(checkIfTableExists("ChicagoCrime_Staging")):
        #drop table
        dropTable("ChicagoCrime_Staging")
    print("About to create table")
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


def insertDataToTable():
    data = getMongoData()
    print("Gotten dataframe holding mongo data")
    tuples = [tuple(x) for x in data.to_numpy()]
    cols = ','.join(list(data.columns.str.replace(" ","")))
    createTable()
    insertQuery = "INSERT INTO %s(%s) VALUES %%s" % ("ChicagoCrime_Staging", cols)
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
    insertDataToTable()
