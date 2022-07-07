import facebook
from datetime import datetime, timedelta

import pandas as pd
from facebookSource.ComplexInsight import ComplexInsight
from facebookSource.Insight import Insight
from facebookSource.Post import Post

class Account:
	def __init__(self,id,name,token):

		'''Carga posteos e insights'''

		self.id = id
		self.name = name
		self.token = token

	def getPosts(self,n_days):

		'''Recupera los posteos y los devuelve en un Dataframe para cada etapa: (raw, cleansed, SQL)'''

		graph = facebook.GraphAPI(access_token=self.token, version="3.1")
		posts_data = graph.get_all_connections(
			id=self.id, 
			connection_name='posts', 
			fields='id, message, created_time, updated_time, attachments,insights.metric(post_activity_by_action_type,post_impressions,post_engaged_users)', 
			since=(datetime.now() - timedelta(days=n_days)))
		
		posts_list = list[Post]()
		for i,p in enumerate(posts_data):
			p['pagename'] = self.name
			post = Post(data=p,token=self.token)
			posts_list.append(post)

		return posts_list

	def getInsights(self,n_days=10,since=None) -> list[Insight]:
		insights = []
		filepath = 'facebookSource/insights.csv'
		csv = pd.read_csv(filepath)
		for index, row in csv.iterrows():
			insights.append(Insight(row['insight'],self.id,self.token,self.name,n_days,since))
		return insights

	def getComplexInsights(self,n_days=10,since=None) -> list[ComplexInsight]:
		insights = []
		filepath = 'facebookSource/complex_insights.csv'
		csv = pd.read_csv(filepath)
		for index, row in csv.iterrows():
			insights.append(ComplexInsight(row['insight'],self.id,self.token,self.name,n_days,since))
		return insights