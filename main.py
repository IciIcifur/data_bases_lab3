import os

from libraries import psycopg2_main
from libraries import SQLite_main
from libraries import DuckDB_main
from libraries import Pandas_main

os.system('pip install -r requirements.txt')

results = open('results.txt', 'w+')
results.write('LIBRARY|tiny 1|tiny 2|tiny 3|tiny 4|big 1|big 2|big 3|big 4')
results.close()

libs = [psycopg2_main, SQLite_main, DuckDB_main, Pandas_main]

config = open('config.txt', "r")
for line in config.readlines():
    if line[0] == '+':
        print("\nRunning queries through " + line[1:-1] + ".")

        lib = libs[0]
        if line[1] == 'p':
            lib = libs[0]
        elif line[1] == 'S':
            lib = libs[1]
        elif line[1] == 'D':
            lib = libs[2]
        elif line[1] == 'P':
            lib = libs[3]

        results = open('results.txt', 'a')
        results.write('\n' + line[1:-1])
        results.close()
        lib.main('nyc_yellow_tiny', 'libraries/data/nyc_yellow_tiny.csv')
        if lib != psycopg2_main:
            lib.main('nyc_yellow_big', 'libraries/data/nyc_yellow_big.csv')

results.close()
