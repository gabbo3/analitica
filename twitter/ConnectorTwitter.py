import json
import logging
from databases.MongoDB.MongoDB import MongoDB
from databases.SQLServer.SQLServer import SQLServer
from connectors.Connector import Connector
from twitter.Account import Account

class ConnectorTwitter(Connector):
	def __init__(self) -> None:
		# Autenticar las cuentas
		self.mongo = MongoDB()
		self.sql = SQLServer()
		self.accounts = self.loadAccounts()

	def execute(self):
		# Para cada cuenta
		for a in self.accounts:
			logging.info('Procesando: ' + a.name)
			logging.info('Recuperando tweets de : ' + a.name)
			tweets = a.getTweets()
			for t in tweets:
				self.sql.upsert(t.asSQLDF(), 'PY_TW_TWEETS')

			logging.info('Recuperando menciones a : ' + a.name)
			mentions = a.getMentions()
			for t in mentions:
				self.sql.upsert(t.asSQLDF(), 'PY_TW_MENTIONS')
			
		logging.info('Recuperando datos del negocio')
		users = self.accounts[0].getUsers()
		for u in users:
			self.sql.upsert(u.asSQLDF(), 'PY_TW_USERS')

	def loadAccounts(self) -> list[Account]:
		retval = list[Account]()

		with open('twitter/accounts.json') as json_file:
			data = json.load(json_file)

		for i in data['Accounts']:
			retval.append(Account(i))

		return retval