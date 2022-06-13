import pandas as pd
from mappings.twitter.tweets import TweetMap

class Tweet:
	def __init__(self, data : dict) -> None:
		self.data = data

	def asSQLDF(self):
		return TweetMap.sql(pd.json_normalize(self.data))