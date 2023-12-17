import csv
from datetime import datetime
from libraries.additionals import dataType

import sqlite3 as sl


def fillTheDatabase(database, path):
    dbPath = 'databases/' + database + '.db'
    connection = sl.connect(dbPath)

    print("Database created succesfully.")
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
            initQuery = 'CREATE TABLE ' + database + ' ('

            for column in columns:
                dataTypes.append(dataType.check(line[j]))
                initQuery += column + ' ' + dataTypes[j] + ' NULL, '
                j += 1

            initQuery = initQuery[0:-2]
            initQuery += ')'
            cursor.execute(initQuery)

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
        connection.commit()
        if (i + 1) % 10000 == 0:
            print(i + 1, 'lines are already processed...')

    print('The table filled succesfully.')
    cursor.close()
    connection.close()


def fourQueries(database):
    dbPath = 'databases/' + database + '.db'
    connection = sl.connect(dbPath)
    cursor = connection.cursor()

    queries = [
        "SELECT vendorid, count(*) FROM " + database + " GROUP BY 1;",
        "SELECT passenger_count, avg(total_amount) FROM " + database + " GROUP BY 1;",
        "SELECT passenger_count, strftime('%Y', tpep_pickup_datetime) AS 'Year', count(*) FROM " + database + " GROUP BY 1, 2",
        "SELECT passenger_count, strftime('%Y', tpep_pickup_datetime) AS 'Year', round(trip_distance), count(*) "
        "FROM " + database + " GROUP BY 1, 2, 3 ORDER BY 2, 4 desc;"
    ]

    results = open('results.txt', 'a')

    for query in queries:
        print("Next query...")
        times = []

        for t in range(100):
            start = datetime.now()

            cursor.execute(query)

            end = datetime.now()

            times.append((end - start).total_seconds() * 1000)

        results.write(f"|{(sum(times) / 100):.5}")

    cursor.close()
    connection.close()
    results.close()


def main(database, path):
    try:
        fillTheDatabase(database, path)
    except:
        print('Database is already filled.')
    finally:
        fourQueries(database)
