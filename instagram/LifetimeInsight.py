import pandas as pd
from instagram.Insight import Insight
from mappings.instagram.Insight import InsightMapping


class LifetimeInsight(Insight):
	def asRawDF(self) -> pd.DataFrame:
		return InsightMapping.raw_lifetime(self.data)

	def asSQLDF(self) -> pd.DataFrame:
		return InsightMapping.sql_lifetime(self.data)