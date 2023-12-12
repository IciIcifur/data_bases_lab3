import subprocess
import os

results = open('libraries/results.txt', 'w+')
results.write('LIBRARY|1 query|2 query|3 query|4 query\n')
results.close()

config = open('config.txt', "r")
for line in config.readlines():
    if line[0] == '+':
        print("Running queries through " + line[1:-1] + ".")
        lib = "libraries/" + line[1:-1] + "_main.py"
        if os.path.exists(lib):
            subprocess.run(['python', lib])

'''
4QUERIES
queries = [
        "SELECT vendorid, count(*) FROM nyc_yellow GROUP BY 1;",
        "SELECT passenger_count, avg(total_amount) FROM nyc_yellow GROUP BY 1;",
        "SELECT passenger_count, extract(year from tpep_pickup_datetime), count(*) FROM nyc_yellow GROUP BY 1, 2",
        "SELECT passenger_count, extract(year from tpep_pickup_datetime), round(trip_distance), count(*) "
        "FROM nyc_yellow GROUP BY 1, 2, 3 ORDER BY 2, 4 desc"
    ]'''