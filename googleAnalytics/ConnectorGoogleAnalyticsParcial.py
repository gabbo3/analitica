from datetime import datetime
from dateutil.relativedelta import relativedelta
import json
from googleAnalytics.ConnectorGoogleAnalytics import ConnectorGoogleAnalytics

class ConnectorGoogleAnalyticsParcial(ConnectorGoogleAnalytics):

	def execute(self):
		dict2load = {}
		dict2load['id'] = self.end_date
		dict2load['Results'] = {}
		for q in self.queries:
			dict2load['Results'][q.name] = []

		for q in self.queries:
			q.get_results(self.service,self.start_date,self.end_date)
			self.sql.upsert(q.asSQLDF(),'PY_GA_MENSUALPARCIAL' + q.table)
			dict2load['Results'][q.name].append(json.loads(q.asRawDF().to_json(orient='records')))
		self.mongo.upsertDict(dict2load, 'TESTE', 'GoogleAnalyticsMensualParcial')

	def set_dates(self):
		# 1ro del mes
		self.start_date = format(datetime.today().replace(day=1), '%Y-%m-%d')
		# Dia de ayer
		self.end_date = format(datetime.today() - relativedelta(days=1), '%Y-%m-%d')