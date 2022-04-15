from pyspark import SparkContext
from sys import stdin

if __name__ == "__main__":
	sc = SparkContext("local[*]", "WordCount")

	sc.setLogLevel("ERROR")

	input = sc.textFile("C:/Users/Akhil/Desktop/Datasets/search_data.txt")

	words = input.flatMap(lambda x: x.split(" "))

	wordsLower = words.map(lambda x: x.lower())

	words_Map = wordsLower.map(lambda x: (x, 1))

	final_Count = words_Map.reduceByKey(lambda x, y: x + y)

	result = final_Count.sortBy(lambda x : x[1], False).collect()

	for i in result:
		print(i)

# stdin.readline()