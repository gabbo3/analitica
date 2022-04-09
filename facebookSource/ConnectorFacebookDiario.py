import pandas as pd
from facebookSource.ConnectorFacebook import ConnectorFacebook

class ConnectorFacebookDiario(ConnectorFacebook):
		
	def execute(self):
		for a in self.accounts:

			# Posteos
			posts = a.getPosts(10)

			postsSQL = []
			for p in posts:
				self.mongo.upsertDict(p.asRawDict(),'TESTE','FacebookPosts_raw')
				self.mongo.upsertDict(p.asCleanDict(),'TESTE','FacebookPosts_clean')
				postsSQL.append(p.asSQLDict())
			
			self.sql.upsert(pd.DataFrame(postsSQL),'PY_FB_POSTS')

			# Insights

			insights = a.getInsights(10)

			for i in insights:
				self.mongo.upsertDF(i.asRawDF(),'TESTE','FacebookInsights_raw')
				self.mongo.upsertDF(i.asCleanDF(),'TESTE','FacebookInsights_clean')
				self.sql.upsert(i.asSQLDF(),'PY_FB_INSIGHTS')
			
			# Complex Insights

			insights = a.getComplexInsights(10)

			for i in insights:
				self.mongo.upsertDF(i.asRawDF(),'TESTE','FacebookInsights_raw')
				self.mongo.upsertDF(i.asCleanDF(),'TESTE','FacebookComplexInsights_clean')
				self.sql.upsert(i.asSQLDF(),'PY_FB_COMPLEX_INSIGHTS')