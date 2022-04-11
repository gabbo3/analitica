from connectors.Connector import Connector
from facebookSource.ConnectorFacebookDiario import ConnectorFacebookDiario
from facebookSource.ConnectorFacebookInsights import ConnectorFacebookInsights


class ConnectorFactory:
    def createConnector(self, name):
        if name == 'facebook':
            return ConnectorFacebookDiario()
        if name == 'facebookinsights':
            return ConnectorFacebookInsights()
        else:
            return Connector()