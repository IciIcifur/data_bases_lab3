import os

from libraries import psycopg2_main
from libraries import SQLite_main
from libraries import DuckDB_main
from libraries import Pandas_main

os.system('pip install -r requirements.txt')

results = open('libraries/results.txt', 'w+')
results.write('LIBRARY|tiny 1|tiny 2|tiny 3|tiny 4|big 1|big 2|big 3|big 4\n')

libs = [psycopg2_main, SQLite_main, DuckDB_main, Pandas_main]

config = open('config.txt', "r")
for line in config.readlines():
    if line[0] == '+':
        print("Running queries through " + line[1:-1] + ".")

        lib = libs[0]
        if line[1] == 'p':
            lib = libs[0]
        elif line[1] == 'S':
            lib = libs[1]
        elif line[1] == 'D':
            lib = libs[2]
        elif line[1] == 'P':
            lib = libs[3]

        results.write(line[1:-1])
        lib.main('nyc_yellow_tiny', 'libraries/data/nyc_yellow_tiny.csv')
        if lib != psycopg2_main:
            lib.main('nyc_yellow_big', 'libraries/data/nyc_yellow_big.csv')
        results.write('\n')
results.close()

'''
4QUERIES
queries = [
        "SELECT vendorid, count(*) FROM nyc_yellow GROUP BY 1;",
        "SELECT passenger_count, avg(total_amount) FROM nyc_yellow GROUP BY 1;",
        "SELECT passenger_count, extract(year from tpep_pickup_datetime), count(*) FROM nyc_yellow GROUP BY 1, 2;",
        "SELECT passenger_count, extract(year from tpep_pickup_datetime), round(trip_distance), count(*) "
        "FROM nyc_yellow GROUP BY 1, 2, 3 ORDER BY 2, 4 desc;"
    ]'''
