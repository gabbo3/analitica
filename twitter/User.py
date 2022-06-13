import pandas as pd
from mappings.twitter.users import UsersMap


class User:
	def __init__(self, data : dict) -> None:
		self.data = data

	def asSQLDF(self):
		return UsersMap.sql(pd.json_normalize(self.data))