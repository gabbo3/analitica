from connectors.Connector import Connector
from facebookSource.ConnectorFacebookDiario import ConnectorFacebookDiario
from facebookSource.ConnectorFacebookInsights import ConnectorFacebookInsights
from googleAdServer.ConnectorGoogleAd import ConnectorGoogleAd
from googleAnalytics.ConnectorGoogleAnalyticsDiario import ConnectorGoogleAnalyticsDiario
from googleAnalytics.ConnectorGoogleAnalyticsMensual import ConnectorGoogleAnalyticsMensual
from googleAnalytics.ConnectorGoogleAnalyticsParcial import ConnectorGoogleAnalyticsParcial
from instagram.ConnectorInstagram import ConnectorInstagram
from youtube.ConnectorYouTube import ConnectorYouTube


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
            return ConnectorGoogleAnalyticsDiario()
        elif name == 'googleanalyticsmensual':
            return ConnectorGoogleAnalyticsMensual()
        elif name == 'googleanalyticsparcial':
            return ConnectorGoogleAnalyticsParcial()
        elif name == 'youtube':
            return ConnectorYouTube()
        else:
            return Connector()