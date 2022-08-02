#!/usr/bin/env python

from pyspark.sql import types
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from typing import List

spark = SparkSession.builder \
		.master("local[*]") \
		.config("spark.executor.memory", "2gb") \
		.getOrCreate()


lines_df = spark.read.text('data/wordcount.txt')
lines_df.printSchema()
lines_df.show()


words_df = lines_df.withColumn('word', F.explode(F.split(F.col('value'), ' '))) \
		.groupBy('word').count().sort('count', ascending=False)
words_df.show()

# saves it to MEMORY_AND_DISK
words_df.cache()
assert 381 == words_df.count()


# How many words have more than 4 occurrences
filtered_words_df = words_df.filter(F.col('count') > 4)
assert 26 == filtered_words_df.count()


# # 2. Tweet Mining

tweets_df = spark.read.json('data/reduced-tweets.json')

tweets_df.printSchema()
tweets_df.show()

# Find all the persons mentioned on tweets
def extract_mentions(words: List[str]) -> List[str]:
	mentions = []
	for word in words:
		if len(word) > 1 and word[0] == '@':
			mentions.append(word.lower())

	return mentions


filtered_mentions_df = F.udf(lambda z: extract_mentions(z), types.ArrayType(types.StringType()))
spark.udf.register("filter_mentions", filtered_mentions_df)
mentions_df = tweets_df.withColumn('mention', F.explode(filtered_mentions_df(F.split(F.col('text'), ' ')))).select('mention')
mentions_df.cache()
assert mentions_df.count() == 4462
assert mentions_df.filter(F.col('mention') == "@jordinsparks").count() == 2


# Find all the hashtags mentioned on tweets
@F.udf(returnType=types.ArrayType(types.StringType()))
def extract_hashtags(words: List[str]) -> List[str]:
	mentions = []
	for word in words:
		if len(word) > 1 and word[0] == '#':
			mentions.append(word.lower())

	return mentions


hashtags_df = tweets_df.select('text').select(F.explode(extract_hashtags(F.split(F.col('text'), ' '))))

hashtags_df.cache()
assert hashtags_df.count() == 5262
assert hashtags_df.where(F.col('col') == '#youtube').count() == 4


# Find same hashtags and mentions
hashtags_stripped_df = hashtags_df.drop_duplicates().select(F.substring('col', pos=2, len=1000).alias("text"))
mentions_stripped_df = mentions_df.drop_duplicates().select(F.substring('mention', pos=2, len=1000).alias("text"))

same_hashtags_mentions_df = hashtags_stripped_df.join(other=mentions_stripped_df,
													  on=hashtags_stripped_df.text == mentions_stripped_df.text,
													  how="inner").drop(mentions_stripped_df.text)

assert same_hashtags_mentions_df.count() == 39

