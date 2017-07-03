from pyspark import SparkContext
from pyspark.sql import HiveContext
from datetime import date, datetime
from datetime import timedelta       ## to calculate the time interval
from geopy.distance import vincenty  ## module to calculate the distance between two points

def trips_mapper(splitIndex, iterator):
    '''
    A function to select citibike trips on feb 1st 2015 at Greenwich station
    to collect the starttime of the selected trips
    Returns: the starttime and the ride index
    '''
    if splitIndex == 0:
        iterator.next()
    import csv
    reader = csv.reader(iterator)
    for row in reader:
    	## select the data Feb 1st 2015 and Greenwich Ave & 8 Ave station
        if (row[3][:10] == '2015-02-01') & (row[6] == 'Greenwich Ave & 8 Ave'):
            a = datetime.strptime(row[3], "%Y-%m-%d %H:%M:%S+%f")
            yield a, row[0]

def select_by_dist(splitIndex, iterator):
    '''
    A function to select the dropoff latitude and longitude for all the yellow taxi
    trips of Feb 1 2015 withn 0.25 miles of the selected station for citibike
    Returns: the dropoff time and the 10 min time interval after the dropoff
    '''
    if splitIndex == 0:
        iterator.next()
    import csv
    reader = csv.reader(iterator)
    for row in reader:
    	## validity check
        if (row[4] != 'NULL') & (row[5] != 'NULL'):
            ## the coordinates of Greenwich station are 40.73901691,-74.00263761
            if (vincenty((40.73901691,-74.00263761), (float(row[4]), float(row[5]))).miles) <= 0.25:
                ## check the distance is within 0.25 miles 
                ## select the time starttime of the trip
                a = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S.%f")
                b = a + timedelta(seconds = 600)
                yield a,b

sc = SparkContext()                
spark = HiveContext(sc)

## read the yellow taxi data from cluster location
taxi = sc.textFile('/tmp/yellow.csv.gz').cache()

## read the citibike data from cluster location
citibike = sc.textFile('/tmp/citibike.csv').cache()

## run the rdd piplines for citike and yellow taxi
citi_trip = citibike.mapPartitionsWithIndex(trips_mapper)
taxi_dist = taxi.mapPartitionsWithIndex(select_by_dist)

## Create dataframe from the with starttime and ride number 
## the ride no. is added just to create a dataframe and get the count
bike_df = citi_trip.toDF(['start', 'ride_no'])

## Create a dataframe with dropoff time and 10 min time interval 
taxi_df = taxi_dist.toDF(['dropoff_time', 'tenmin_time'])

## join the dataframes with the filter of time interval
final_df = taxi_df.join(bike_df).filter((taxi_df.dropoff_time < bike_df.start) & (taxi_df.tenmin_time > bike_df.start))

## covert the spark dataframe to pandas dataframe and print the unique ride 
print len(final_df.toPandas()['ride_no'].unique())


