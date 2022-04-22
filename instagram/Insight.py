import pandas as pd


class Insight:
	def __init__(self,data : dict) -> None:
		self.data = data

	def asRawDF(self) -> pd.DataFrame:
		return pd.DataFrame(self.data)

	def asSQLDF(self) -> pd.DataFrame:
		return pd.DataFrame(self.data)