import pandas as pd
from mappings.twitter.mentions import MentionMap

class Mention:
	def __init__(self, data) -> None:
		self.data = data

	def asSQLDF(self):
		return MentionMap.sql(pd.json_normalize(self.data))