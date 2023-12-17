from datetime import datetime

import pandas as pd


def fillTheDatabase(path):
    frame = pd.read_csv(path, sep=',')
    dates = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
    frame[dates] = frame[dates].astype("datetime64[ns]")
    print("Database created and filled succesfully.")
    return frame


def fourQueries(frame):
    results = open('results.txt', 'a')

    print("Next query...")
    times = []
    for t in range(10):
        start = datetime.now()
        frame.groupby('VendorID').size()
        end = datetime.now()

        times.append((end - start).total_seconds())
    results.write(f"|{(sum(times) / 10):.5}")

    print("Next query...")
    times = []
    for t in range(10):
        start = datetime.now()

        frame.groupby('passenger_count')['total_amount'].mean()

        end = datetime.now()

        times.append((end - start).total_seconds())
    results.write(f"|{(sum(times) / 10):.5}")

    print("Next query...")
    times = []
    for t in range(10):
        start = datetime.now()

        frame.groupby(['passenger_count', frame['tpep_pickup_datetime'].dt.to_period("Y")]).size()

        end = datetime.now()

        times.append((end - start).total_seconds())
    results.write(f"|{(sum(times) / 10):.5}")

    print("Next query...")
    times = []
    for t in range(10):
        start = datetime.now()
        frame.sort_values('tpep_pickup_datetime').groupby(
            ['passenger_count', frame['tpep_pickup_datetime'].dt.to_period("Y"),
             frame['trip_distance'].round()]).size().sort_values(ascending=False)
        end = datetime.now()

        times.append((end - start).total_seconds())
    results.write(f"|{(sum(times) / 10):.5}")

    results.close()


def main(database, path):
    db_frame = fillTheDatabase(path)
    fourQueries(db_frame)
