from pyspark import SparkContext
from sys import stdin

if __name__ == "__main__":
	sc = SparkContext("local[*]", "WordCount")

	sc.setLogLevel("ERROR")

	input = sc.textFile("C:/Users/Akhil/Desktop/Datasets/search_data.txt")

	words = input.flatMap(lambda x: x.split(" "))

	wordsLower = words.map(lambda x: x.lower())

	words_Map = wordsLower.map(lambda x: (x, 1))

	final_Count = words_Map.reduceByKey(lambda x, y: x + y).map(lambda x: (x[1], x[0]))

	result = final_Count.sortByKey(False).map(lambda x:(x[1],x[0])).collect()

	for i in result:
		print(i)

stdin.readline()