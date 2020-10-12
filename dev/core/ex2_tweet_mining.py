from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession


def extract_mentions(words):
	mentions = []
	for word in words:
		if len(word) > 1 and word[0] == '@':
			mentions.append(word)

	return mentions


def main():
	spark = SparkSession.builder \
		.master("local[*]") \
		.config("spark.executor.memory", "1gb") \
		.getOrCreate()

	tweets = spark.read.json('../../data/reduced-tweets.json')

	filter_mentions = udf(lambda z: extract_mentions(z), ArrayType(StringType()))
	spark.udf.register("filter_mentions", filter_mentions)

	# Find all the persons mentioned on tweets (case sensitive)
	mentions = tweets.withColumn('mention', explode(filter_mentions(split(col('text'), ' '))))\
		.select('mention')

	mentions.cache()
	assert mentions.count() == 4462

	assert mentions.filter(col('mention') == "@JordinSparks").count() == 2

	# Count how many times each person is mentioned
	count_mentions = mentions.groupBy('mention').count()
	count_mentions.cache()
	assert count_mentions.count() == 3283
	assert count_mentions.filter(col('mention') == "@JordinSparks").count() == 1
	assert count_mentions.filter(col('mention') == "@JordinSparks").collect()[0].asDict()['count'] == 2

	# Find the 10 most mentioned persons by descending order
	top10 = count_mentions.sort('count', ascending=False).take(10)
	assert top10[0].asDict()['count'] == 189
	assert top10[0].asDict()['mention'] == '@ShawnMendes'
	assert top10[1].asDict()['count'] == 100
	assert top10[1].asDict()['mention'] == '@HIITMANonDECK'

	print('SUCCESS')
if __name__ == '__main__':
	main()
