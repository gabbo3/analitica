from datetime import datetime, timedelta
from connectors.Connector import Connector
from databases.MongoDB.MongoDB import MongoDB
from databases.SQLServer.SQLServer import SQLServer
from googleAnalytics.Account import Account
from googleAnalytics.Query import Query, TraficoSinPlayerCienRadios, TraficoSinPlayerMitre, TraficoTotal,TraficoXCanal,TraficoXPais,TraficoXPaisDispositivo,TraficoXDispositivo,TraficoXFuente,TraficoTotalXRRSS,TraficoPlayersRedCienRadios,TraficoPlayersLa100,TraficoSinPlayerLa100,TraficoPlayersMitre,TraficoPlayersCienradios,TraficoPlayerLa100Alternativo,TraficoPlayersLa100XPlayer,TraficoTotalPlayersLa100,TraficoXHostnameADC,TraficoTotalHostnameADC,TraficoXHostnameVertical,TraficoTotalHostnameVertical
from googleAnalytics.Service import Service
import json

class ConnectorGoogleAnalytics(Connector):
	def __init__(self) -> None:
		self.mongo = MongoDB()
		self.sql = SQLServer()
		self.accounts = self.loadAccounts()
		self.loadQueries()
		self.set_dates()
		self.service = Service()

	def loadAccounts(self) -> list[Account]:
		retval = list[Account]()

		with open('googleAnalytics/accounts.json') as json_file:
			data = json.load(json_file)

		for i in data['Accounts']:
			retval.append(Account(i))

		return retval

	def loadQueries(self):
		self.queries = list[Query]()
		for a in self.accounts:
			self.queries.append(TraficoTotal(a.id))
			self.queries.append(TraficoXCanal(a.id))
			self.queries.append(TraficoXPais(a.id))
			self.queries.append(TraficoXPaisDispositivo(a.id))
			self.queries.append(TraficoXDispositivo(a.id))
			self.queries.append(TraficoXFuente(a.id))
			self.queries.append(TraficoTotalXRRSS(a.id))

		self.queries.append(TraficoPlayersRedCienRadios())
		self.queries.append(TraficoPlayersLa100())
		self.queries.append(TraficoSinPlayerLa100())
		self.queries.append(TraficoSinPlayerMitre())
		self.queries.append(TraficoSinPlayerCienRadios())
		self.queries.append(TraficoPlayersMitre())
		self.queries.append(TraficoPlayersCienradios())
		self.queries.append(TraficoPlayerLa100Alternativo())
		self.queries.append(TraficoPlayersLa100XPlayer())
		self.queries.append(TraficoTotalPlayersLa100())
		self.queries.append(TraficoXHostnameADC())
		self.queries.append(TraficoTotalHostnameADC())
		self.queries.append(TraficoXHostnameVertical())
		self.queries.append(TraficoTotalHostnameVertical())
