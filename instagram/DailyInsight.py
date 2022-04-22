import pandas as pd
from instagram.Insight import Insight
from mappings.instagram.Insight import InsightMapping


class DailyInsight(Insight):
	def asRawDF(self) -> pd.DataFrame:
		return InsightMapping.raw_daily(self.data)

	def asSQLDF(self) -> pd.DataFrame:
		return InsightMapping.sql_daily(self.data)