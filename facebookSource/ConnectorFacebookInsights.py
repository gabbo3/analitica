import logging
from facebookSource.ConnectorFacebook import ConnectorFacebook

class ConnectorFacebookInsights(ConnectorFacebook):
	
	def execute(self):
		for a in self.accounts:
			logging.info('Procesando: ' + a.name)
			# Simple Insights
			logging.info('Recuperando estadisticas de: ' + a.name)
			insights = a.getInsights(since='2020-01-01')

			for i in insights:
				self.mongo.upsertDF(i.asRawDF(),'RAWDATA','FacebookInsights')
				# self.mongo.upsertDF(i.asCleanDF(),'CLEANSED','FacebookInsights_clean')
				self.sql.upsert(i.asSQLDF(),'PY_FB_INSIGHTS')

			# Complex Insights
			logging.info('Recuperando estadisticas complejas de: ' + a.name)
			insights = a.getComplexInsights(since='2020-01-01')

			for i in insights:
				self.mongo.upsertDF(i.asRawDF(),'RAWDATA','FacebookInsights')
				# self.mongo.upsertDF(i.asCleanDF(),'TESTE','FacebookComplexInsights_clean')
				self.sql.upsert(i.asSQLDF(),'PY_FB_COMPLEX_INSIGHTS')