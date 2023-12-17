import csv
from datetime import datetime
from libraries.additionals import dataType
import psycopg2 as dbapi


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

    newTableQuery = "CREATE TABLE " + database + "()"
    cursor.execute(newTableQuery)
    connection.commit()

    print("Database and new table were created successfully.")

    cursor.close()
    connection.close()


def fillTheDatabase(database, user, password, path):
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
                dataTypes.append(dataType.check(line[j]))
                cursor.execute(
                    "ALTER TABLE " + database +
                    " ADD " + column + " " + dataTypes[j] + " NULL;")
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
        if (i + 1) % 400000 == 0:
            print(i + 1, 'lines are already processed...')

    connection.commit()
    cursor.close()
    connection.close()


def fourQueries(database, user, password):
    connection = dbapi.connect(database=database, user=user, password=password)
    cursor = connection.cursor()

    queries = [
        "SELECT vendorid, count(*) FROM " + database + " GROUP BY 1;",
        "SELECT passenger_count, avg(total_amount) FROM " + database + " GROUP BY 1;",
        "SELECT passenger_count, extract(year from tpep_pickup_datetime), count(*) FROM " + database + " GROUP BY 1, 2",
        "SELECT passenger_count, extract(year from tpep_pickup_datetime), round(trip_distance), count(*) "
        "FROM " + database + " GROUP BY 1, 2, 3 ORDER BY 2, 4 desc"
    ]

    results = open('results.txt', 'a')

    for query in queries:
        print("Next query...")
        times = []
        for t in range(10):
            start = datetime.now()

            cursor.execute(query)

            end = datetime.now()
            times.append((end - start).total_seconds())
        results.write(f"|{(sum(times) / 10):.5}")

    connection.commit()
    cursor.close()
    connection.close()
    results.close()


def main(database, path):
    try:
        createTheDatabase(database, 'postgres', '123')
    except:
        print('Database and table are already exist.')
    finally:
        try:
            fillTheDatabase(database, 'postgres', '123', path)
        except:
            print('Database is already filled.')
        finally:
            try:
                fourQueries(database, 'postgres', '123')
            except:
                print('An error occured while working with psycopg2. Try to delete the database in PGAdmin.')
