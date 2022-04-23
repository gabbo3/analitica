from connectors.Connector import Connector
from facebookSource.ConnectorFacebookDiario import ConnectorFacebookDiario
from facebookSource.ConnectorFacebookInsights import ConnectorFacebookInsights
from googleAdServer.ConnectorGoogleAd import ConnectorGoogleAd
from instagram.ConnectorInstagram import ConnectorInstagram


class ConnectorFactory:
    def createConnector(self, name):
        if name == 'facebook':
            return ConnectorFacebookDiario()
        elif name == 'facebookinsights':
            return ConnectorFacebookInsights()
        elif name == 'instagram':
            return ConnectorInstagram()
        elif name == 'googleadserver':
            return ConnectorGoogleAd()
        else:
            return Connector()