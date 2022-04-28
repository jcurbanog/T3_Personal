import os
import pandas as pd
from PyQt5.QtSql import QSqlDatabase, QSqlQuery

DATES = ["2019-08-27 14:00:00", "2019-08-27 14:15:00", "2016-07-11 21:45:00","2019-08-27 14:00:00","2019-08-27 18:15:00"]

def load_pandas(db, statement, cols):    
        db.transaction()
        query = QSqlQuery(db)
        query.exec(statement)
        table = []
        while query.next():
            values = []
            for i in range(query.record().count()):
                values.append(query.value(i))
            table.append(values)
        df = pd.DataFrame(table)
        # for i in range(0, len(cols)) :
        #     df.columns.values[i] = cols[i]
        try:
            df.columns = cols
        except ValueError:
            pass
        db.commit()
        return df

sqlfile = "test.sqlite"
if os.path.exists(sqlfile):
    os.remove(sqlfile)
        
con = QSqlDatabase.addDatabase("QSQLITE")
con.setDatabaseName(sqlfile)
if not con.open():
    print("Cannot open SQL database")

dropTableQuery = QSqlQuery(con) # Connection mustt be specified in each query
dropTableQuery.exec("DROP TABLE IF EXISTS DatesTable") 
dropTableQuery.finish()

# Create the table for storing the temperature measures (id, date, temp*4)
createTableQuery = QSqlQuery(con) 
# Columns and their data type are defined
createTableQuery.exec(
    """
    CREATE TABLE IF NOT EXISTS DatesTable (
        id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL,
        date TIMESTAMP,  
        UNIQUE(date)    
    )
    """
)

createTableQuery.finish()

# Construct the dynamic insert SQL request and execute it        
dynamicInsertQuery = QSqlQuery(con)
# In a dynamic query, first of all the query is prepared with placeholders (?)
dynamicInsertQuery.prepare(
    """
    INSERT INTO DatesTable (
        date
    )
    VALUES (?)

    """
)


for i in range(len(DATES)): 
    val = DATES[i]  # Each row of the DataFrame is selected as a tuple
    dynamicInsertQuery.addBindValue(str(val)) # The first placeholder is for the date. Then, it should be a string
    # dynamicInsertQuery.addBindValue(str(val))
    a = dynamicInsertQuery.exec() # Once the placeholders are filled, the query is executed
    if not a:
        # print(dynamicInsertQuery.lastError().text())
        pass
dynamicInsertQuery.finish()

statement = 'SELECT id, date FROM DatesTable'
cols = ["id", "date"]
print(load_pandas(con, statement, cols))