from facebookSource.ConnectorFacebook import ConnectorFacebook

class ConnectorFacebookInsights(ConnectorFacebook):
	
	def execute(self):
		for a in self.accounts:
			# Simple Insights

			insights = a.getInsights(since='2020-01-01')

			for i in insights:
				self.mongo.upsertDF(i.asRawDF(),'TESTE','FacebookInsights_raw')
				self.mongo.upsertDF(i.asCleanDF(),'TESTE','FacebookInsights_clean')
				self.sql.upsert(i.asSQLDF(),'PY_FB_INSIGHTS')

			# Complex Insights

			insights = a.getComplexInsights(since='2020-01-01')

			for i in insights:
				self.mongo.upsertDF(i.asRawDF(),'TESTE','FacebookInsights_raw')
				self.mongo.upsertDF(i.asCleanDF(),'TESTE','FacebookComplexInsights_clean')
				self.sql.upsert(i.asSQLDF(),'PY_FB_COMPLEX_INSIGHTS')