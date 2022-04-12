import logging
from connectors.Connector import Connector
from databases.MongoDB.MongoDB import MongoDB
from databases.SQLServer.SQLServer import SQLServer
from googleAdServer.ReportNoVideo import ReportNoVideo
from googleAdServer.ReportVideo import ReportVideo


class ConnectorGoogleAd(Connector):
	def __init__(self, name):
		super().__init__(name)
		self.mongo = MongoDB()
		self.sql = SQLServer()
	
	def execute(self):
		try:
			logging.info('dimensionsNoVideo')
			firstReport = ReportNoVideo()
			firstReport.run()
			self.mongo.upsertDF(firstReport.asRawDF(),'base','collection')
			self.mongo.upsertDF(firstReport.asCleanDF(),'base','collection')
			self.sql.upsert(firstReport.asSQLDF(),'PY_DFP_Impresiones')
		except Exception as e:
			logging.error('Error: Report No Video')
		try:
			logging.info('dimensionsVideo')
			secondReport = ReportVideo()
			secondReport.run()
			self.mongo.upsertDF(secondReport.asRawDF(),'base','collection')
			self.mongo.upsertDF(secondReport.asCleanDF(),'base','collection')
			self.sql.upsert(secondReport.asSQLDF(),'PY_DFP_Impresiones_videos')
		except Exception as e:
			logging.error('Error: Report Video')
	