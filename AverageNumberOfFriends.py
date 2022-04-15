
from pyspark import SparkContext


def parse_line(lines):
	fields = lines.split("::")
	age = int(fields[2])
	no_of_friends = int(fields[3])
	return (age, no_of_friends)


sc = SparkContext("local[*]", "friendsByAge")

lines = sc.textFile("C:/Users/Akhil/Desktop/Datasets/friendsdata.csv")

rdd = lines.map(parse_line)

totals_by_age = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

averages_by_age = totals_by_age.mapValues(lambda x: x[0] / x[1])

results = averages_by_age.collect()

for i in results:
	print(i)