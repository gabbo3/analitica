from datetime import datetime, timedelta
from connectors.Connector import Connector
from databases.MongoDB.MongoDB import MongoDB
from databases.SQLServer.SQLServer import SQLServer
from googleAnalytics.Account import Account
from googleAnalytics.Query import Query
from googleAnalytics.Service import Service
import json

class ConnectorGoogleAnalytics(Connector):
    def __init__(self) -> None:
        # self.mongo = MongoDB()
        # self.sql = SQLServer()
        self.queries = self.loadQueries()
        self.accounts = self.loadAccounts()
        self.set_dates()
        
    def execute(self):
        # Definir un servicio
        service = Service()
        for q in self.queries:
            for a in self.accounts:
                try:
                    df = service.query(ids=a.id,start_date=self.start_date, end_date= self.end_date,
                        metrics=q.metrics, 
                        dimensions=q.dimensions,
                        filters=q.filters)
                    print(df.to_csv())
                except Exception as e:
                    print(e.args)
        #     # Subir el resultado
        #     self.mongo.upsertDF(df,'TESTE', 'GoogleAnalytics_raw')
        #     self.sql.upsert(df,'PY_GA_DIARIO')

        # pass

    def loadAccounts(self) -> list[Account]:
        retval = list[Account]()

        with open('googleAnalytics/accounts.json') as json_file:
            data = json.load(json_file)

        for i in data['Accounts']:
            retval.append(Account(i))

        return retval

    def loadQueries(self) -> list[Query]:
        retval = list[Query]()

        with open('googleAnalytics/queries.json') as json_file:
            data = json.load(json_file)

        for i in data['Queries']:
            retval.append(Query(i))

        return retval

    def set_dates(self):
        self.start_date = format(datetime.today() - timedelta(days=1), '%Y-%m-%d')
        self.end_date = format(datetime.today() - timedelta(days=1), '%Y-%m-%d')