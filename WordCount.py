from pyspark import SparkContext

sc = SparkContext("local[*]","WordCount")

input = sc.textFile("C:/Users/Akhil/Desktop/Datasets/search_data.txt")

words = input.flatMap(lambda x: x.split(" "))

# wordsLower = words.map(lambda x: x.toLowerCase())

words_Map = words.map(lambda x: (x, 1))

final_Count = words_Map.reduceByKey(lambda x, y: x + y)

# sortedResults = final_Count.sortBy()

result = final_Count.collect()

for i in result:
	print(i)