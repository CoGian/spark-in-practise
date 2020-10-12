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

	# for each tweet, extract all the hashtag and then create couples (hashtag,tweet)
	hashtag_mentions = tweets.withColumn('hashtag', explode(filter_hashtags(split(col('text'), ' '))))
	hashtag_mentions = hashtag_mentions.select('hashtag', to_json(struct('id', 'user', 'country', 'place', 'text')).alias('tweet'))
	hashtag_mentions_grouped_by_hashtag = hashtag_mentions.groupBy('hashtag').agg(collect_list(col('tweet')))

	hashtag_mentions_grouped_by_hashtag.cache()

	assert hashtag_mentions_grouped_by_hashtag.count() == 2461
	hashtag_mentions_grouped_by_hashtag_dict = dict(hashtag_mentions_grouped_by_hashtag.collect())
	paris = hashtag_mentions_grouped_by_hashtag_dict['#Paris']
	assert len(paris) == 144

	edc = hashtag_mentions_grouped_by_hashtag_dict['#EDC']
	assert len(edc) == 1

	print('SUCCESS')


class Tweet:

	def __init__(self, id, user, country, place, text):
		self.id = id
		self.user = user
		self.country = country
		self.place = place
		self.text = text


if __name__ == '__main__':
	main()
