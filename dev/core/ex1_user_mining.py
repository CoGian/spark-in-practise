from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession


def main():
	spark = SparkSession.builder \
		.master("local[*]") \
		.config("spark.executor.memory", "1gb") \
		.getOrCreate()

	tweets = spark.read.json('../../data/reduced-tweets.json')

	tweets.cache()
	# For each user return all his tweets

	tweets_by_user = tweets.groupBy('user').agg(collect_list(col('country')),
												collect_list(col("id")),
												collect_list(col("place")),
												collect_list(col("text")))

	assert tweets_by_user.count() == 5967

	# Compute the number of tweets by user

	tweets_count_by_user = tweets.groupBy('user').agg(count(col("text")))

	result = tweets_count_by_user.filter(col('user') == "Dell Feddi")

	result.cache()
	assert result.count() == 1
	assert result.select(col("count(text)")).collect()[0].asDict()['count(text)'] == 29

if __name__ == '__main__':
	main()
