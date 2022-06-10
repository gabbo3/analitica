import pandas as pd

class Mention:
	def __init__(self, data) -> None:
		self.data = data

	def asSQLDF(self):
		return pd.json_normalize(self.data)