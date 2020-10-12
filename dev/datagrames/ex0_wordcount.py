from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession


def main():
	spark = SparkSession.builder \
		.master("local[*]") \
		.config("spark.executor.memory", "1gb") \
		.getOrCreate()

	lines = spark.read.text('../../data/wordcount.txt')

	words = lines.withColumn('word', explode(split(col('value'), ' '))) \
		.groupBy('word').count().sort('count', ascending=False)

	words.cache()
	assert 381 == words.count()

	filtered_words = words.filter(col('count') > 4)

	assert 26 == filtered_words.count()


if __name__ == '__main__':
	main()
