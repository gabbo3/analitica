import pandas as pd
from connectors.Connector import Connector
from databases.MongoDB.MongoDB import MongoDB
from databases.SQLServer.SQLServer import SQLServer
from facebookSource.Account import Account

class ConnectorFacebook(Connector):
	def __init__(self) -> None:
		self.accounts = self.setAccounts()
		self.mongo = MongoDB()
		self.sql = SQLServer()
		
	def execute(self):
		pass
			
	def setAccounts(self) -> list[Account]:
		filepath = 'facebookSource/accounts.csv'
		csv = pd.read_csv(filepath)
		accounts = []
		for index, row in csv.iterrows():
			accounts.append(Account(row['id'], row['name'],row['token']))
		
		return accounts