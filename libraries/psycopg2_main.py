import psycopg2 as dbapi
import csv
from datetime import datetime


def checkDataType(data):
    for letter in data:
        if letter == '-' or letter == ':':
            return 'TIMESTAMP'
        if letter == '.':
            return 'FLOAT'
        if 'A' <= letter <= 'Z' or 'a' <= letter <= 'z':
            return 'VARCHAR(20)'
    if len(data):
        return 'INTEGER'
    return 'FLOAT'


def fillTheTable(database, user, password, path):
    connection = dbapi.connect(database=database, user=user, password=password)

    cursor = connection.cursor()

    csvObject = csv.reader(open(path, 'r'), dialect='excel', delimiter=',')

    columns = []
    dataTypes = []
    i = -1

    print("Filling the table may be long. Please, wait.")
    for csvLine in csvObject:
        i += 1
        if i == 0:
            columns = list(csvLine)

            for j in range(len(columns)):
                if columns[j] == '':
                    columns[j] = 'Unnamed'
                if columns[j] == 'Airport_fee':
                    columns[j] = 'DuplicatedColumn'
            continue
        if i == 1:
            line = list(csvLine)

            j = 0
            for column in columns:
                dataTypes.append(checkDataType(line[j]))
                cursor.execute(
                    "ALTER TABLE nyc_yellow "
                    "ADD " + column + " " + dataTypes[j] + " NULL;")
                j += 1

        query = "INSERT INTO " + database + " ("
        for k in range(len(columns) - 1):
            query += columns[k] + ', '
        query += columns[-1] + ') VALUES ('
        line = list(csvLine)
        for k in range(len(line) - 1):
            if line[k] == '':
                line[k] = 'NULL'
            if dataTypes[k] == 'VARCHAR(20)' or dataTypes[k] == 'TIMESTAMP':
                query += "'" + line[k] + "'" + ', '
            else:
                query += line[k] + ', '
        if line[-1] == '':
            line[-1] = 'NULL'
        if dataTypes[-1] == 'VARCHAR(20)' or dataTypes[-1] == 'TIMESTAMP':
            query += "'" + line[-1] + "'" + ');'
        else:
            query += line[-1] + ');'

        cursor.execute(query)
        if (i + 1) % 50 == 0:
            print(i, 'lines are already processed...')

    connection.commit()
    cursor.close()
    connection.close()


def createTheDatabase(database, user, password):
    connection = dbapi.connect(dbname="postgres", user=user, password=password, host="127.0.0.1")
    cursor = connection.cursor()
    connection.autocommit = True

    newBaseQuery = "CREATE DATABASE " + database
    cursor.execute(newBaseQuery)

    cursor.close()
    connection.close()

    connection = dbapi.connect(dbname=database, user=user, password=password, host="127.0.0.1")
    cursor = connection.cursor()

    newTableQuery = "CREATE TABLE " + database
    cursor.execute(newTableQuery)
    connection.commit()

    print("База данных и новая таблица успешно созданы.")

    cursor.close()
    connection.close()


def fourQueries(database, user, password):
    connection = dbapi.connect(database=database, user=user, password=password)
    cursor = connection.cursor()

    queries = [
        "SELECT vendorid, count(*) FROM nyc_yellow GROUP BY 1;",
        "SELECT passenger_count, avg(total_amount) FROM nyc_yellow GROUP BY 1;",
        "SELECT passenger_count, extract(year from tpep_pickup_datetime), count(*) FROM nyc_yellow GROUP BY 1, 2",
        "SELECT passenger_count, extract(year from tpep_pickup_datetime), round(trip_distance), count(*) "
        "FROM nyc_yellow GROUP BY 1, 2, 3 ORDER BY 2, 4 desc"
    ]

    results = open('libraries/results.txt', 'a')
    results.write('Psycopg2|')

    for query in queries:
        print("Next query...")
        times = []
        for t in range(100):
            start = datetime.now()

            cursor.execute(query)

            end = datetime.now()
            times.append((end - start).total_seconds() * 1000)
        results.write(f"{(sum(times) / 100):.5}|")

    connection.commit()
    cursor.close()
    connection.close()
    results.close()


try:
    createTheDatabase('nyc_yellow', 'postgres', '123')
except:
    print('Database and table are already exist.')
finally:
    try:
        fillTheTable('nyc_yellow', 'postgres', '123', 'nyc_yellow_tiny.csv')
    except:
        print('Database is already filled.')
    finally:
        fourQueries('nyc_yellow', 'postgres', '123')
