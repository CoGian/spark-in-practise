from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession


def extract_hashtags(words):
	mentions = []
	for word in words:
		if len(word) > 1 and word[0] == '#':
			mentions.append(word)

	return mentions


def main():
	spark = SparkSession.builder \
		.master("local[*]") \
		.config("spark.executor.memory", "1gb") \
		.getOrCreate()

	tweets = spark.read.json('../../data/reduced-tweets.json')

	filter_hashtags = udf(lambda z: extract_hashtags(z), ArrayType(StringType()))
	spark.udf.register("filter_hashtags", filter_hashtags)

	# Find all the hashtags mentioned on tweets
	hashtag_mentions = tweets.select('text').select(explode(filter_hashtags(split(col('text'), ' '))))

	hashtag_mentions.cache()
	assert hashtag_mentions.count() == 5262
	assert hashtag_mentions.where(col('col') == '#youtube').count() == 2

	# Count how many times each hashtag is mentioned
	hashtag_mentions_count = hashtag_mentions.groupBy('col').count()
	hashtag_mentions_count.cache()
	assert hashtag_mentions_count.count() == 2461
	assert hashtag_mentions_count.where(col('col') == '#youtube').count() == 1
	assert hashtag_mentions_count.where(col('col') == '#youtube').collect()[0].asDict()['count'] == 2

	# Find the 10 most popular Hashtags by descending order
	top10 = hashtag_mentions_count.sort('count', ascending=False).take(10)
	assert top10[0].asDict()['count'] == 253
	assert top10[0].asDict()['col'] == '#DME'

	print('SUCCESS')


if __name__ == '__main__':
	main()
