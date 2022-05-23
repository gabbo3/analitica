from datetime import datetime, timedelta
import json
from googleAnalytics.ConnectorGoogleAnalytics import ConnectorGoogleAnalytics

class ConnectorGoogleAnalyticsDiario(ConnectorGoogleAnalytics):

	def execute(self):
		dict2load = {}
		dict2load['id'] = self.end_date
		dict2load['Results'] = {}
		for q in self.queries:
			dict2load['Results'][q.name] = []

		for q in self.queries:
			q.get_results(self.service,self.start_date,self.end_date)
			self.sql.upsert(q.asSQLDF(),'GA_DIARIO' + q.table)
			dict2load['Results'][q.name].append(json.loads(q.asRawDF().to_json(orient='records')))
		self.mongo.upsertDict(dict2load, 'RAWDATA', 'GoogleAnalyticsDiario')

	def set_dates(self):
		# Dia de ayer
		self.start_date = format(datetime.today() - timedelta(days=1), '%Y-%m-%d')
		self.end_date = format(datetime.today() - timedelta(days=1), '%Y-%m-%d')