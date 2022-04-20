import logging
import pandas as pd
from facebookSource.ConnectorFacebook import ConnectorFacebook

class ConnectorFacebookDiario(ConnectorFacebook):
		
	def execute(self):
		for a in self.accounts:
			logging.info('Procesando: ' + a.name)

			# Posteos
			logging.info('Recuperando posteos de: ' + a.name)
			posts = a.getPosts(10)

			postsSQL = []
			logging.info('Subiendo posteos de: ' + a.name + 'a MongoDB')
			for p in posts:
				self.mongo.upsertDict(p.asRawDict(),'RAWDATA','FacebookPosts')
				# self.mongo.upsertDict(p.asCleanDict(),'TESTE','FacebookPosts_clean')
				postsSQL.append(p.asSQLDict())
			
			self.sql.upsert(pd.DataFrame(postsSQL),'FB_POSTS')

			# Insights
			logging.info('Recuperando estadisticas de: ' + a.name)
			insights = a.getInsights(10)

			for i in insights:
				self.mongo.upsertDF(i.asRawDF(),'RAWDATA','FacebookInsights')
				# self.mongo.upsertDF(i.asCleanDF(),'TESTE','FacebookInsights_clean')
				self.sql.upsert(i.asSQLDF(),'FB_INSIGHTS')
			
			# Complex Insights
			logging.info('Recuperando estadisticas complejas de: ' + a.name)
			insights = a.getComplexInsights(10)

			for i in insights:
				self.mongo.upsertDF(i.asRawDF(),'RAWDATA','FacebookInsights')
				# self.mongo.upsertDF(i.asCleanDF(),'TESTE','FacebookComplexInsights_clean')
				self.sql.upsert(i.asSQLDF(),'FB_COMPLEX_INSIGHTS')