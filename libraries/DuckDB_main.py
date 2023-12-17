from datetime import datetime

import duckdb


def fillTheDatabase(database, path):
    connection = duckdb.connect('databases/' + database + '.duckdb')
    cursor = connection.cursor()

    initQuery = 'CREATE TABLE ' + database + ' AS SELECT * FROM read_csv_auto(' + path + ');'
    cursor.execute(initQuery)
    print("Database created and filled succesfully.")

    connection.close()
    cursor.close()


def fourQueries(database):
    connection = duckdb.connect('databases/' + database + '.duckdb')
    cursor = connection.cursor()

    queries = [
        "SELECT vendorid, count(*) FROM " + database + " GROUP BY 1;",
        "SELECT passenger_count, avg(total_amount) FROM " + database + " GROUP BY 1;",
        "SELECT passenger_count, date_part('year', tpep_pickup_datetime), count(*) FROM " + database + " GROUP BY 1, 2",
        "SELECT passenger_count, date_part('year', tpep_pickup_datetime), round(trip_distance), count(*) "
        "FROM " + database + " GROUP BY 1, 2, 3 ORDER BY 2, 4 DESC;"
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
