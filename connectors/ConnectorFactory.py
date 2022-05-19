from connectors.Connector import Connector
from facebookSource.ConnectorFacebookDiario import ConnectorFacebookDiario
from facebookSource.ConnectorFacebookInsights import ConnectorFacebookInsights
from googleAdServer.ConnectorGoogleAd import ConnectorGoogleAd
from googleAnalytics.ConnectorGoogleAnalytics import ConnectorGoogleAnalytics
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
        elif name == 'googleanalyticsdiario':
            return ConnectorGoogleAnalytics()
        else:
            return Connector()