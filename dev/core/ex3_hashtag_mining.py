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


# print('SUCCESS')
if __name__ == '__main__':
	main()
