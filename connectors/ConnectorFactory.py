from connectors.Connector import Connector
from facebookSource.ConnectorFacebook import ConnectorFacebook
from facebookSource.ConnectorFacebookDiario import ConnectorFacebookDiario
from facebookSource.ConnectorFacebookInsights import ConnectorFacebookInsights


class ConnectorFactory:
    def createConnector(self, name):
        if name == 'facebook':
            return ConnectorFacebookDiario()
        if name == 'facebookInsights':
            return ConnectorFacebookInsights()
        else:
            return Connector()