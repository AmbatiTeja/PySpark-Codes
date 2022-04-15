from pyspark import SparkContext
from sys import stdin

if __name__ == "__main__":
	sc = SparkContext("local[*]", "WordCount")

	sc.setLogLevel("ERROR")

	input = sc.textFile("C:/Users/Akhil/Desktop/Datasets/search_data.txt")

	words = input.flatMap(lambda x: x.split(" "))

	words_counts = words.map(lambda x: x.lower())

	final_count = words_counts.countByValue()

	print(final_count)


# 	for i in result:
# 		print(i)
#
# else :
# 	print("Not Executes Directly")

stdin.readline()