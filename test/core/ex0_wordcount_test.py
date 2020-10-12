import unittest

class ex0_wordcloud_test(unittest.TestCase):

	def test_word_count(self, words):
		assert  26 == words.count()
