from connectors.Connector import Connector
from facebookSource.ConnectorFacebookDiario import ConnectorFacebookDiario
from facebookSource.ConnectorFacebookInsights import ConnectorFacebookInsights
from facebookSource.ConnectorFacebookPosts import ConnectorFacebookPosts
from googleAdServer.ConnectorGoogleAd import ConnectorGoogleAd
from googleAnalytics.ConnectorGoogleAnalyticsDiario import ConnectorGoogleAnalyticsDiario
from googleAnalytics.ConnectorGoogleAnalyticsMensual import ConnectorGoogleAnalyticsMensual
from googleAnalytics.ConnectorGoogleAnalyticsParcial import ConnectorGoogleAnalyticsParcial
from instagram.ConnectorInstagram import ConnectorInstagram
from instagram.ConnectorInstagramHistorico import ConnectorInstagramHistorico
from twitter.ConnectorTwitter import ConnectorTwitter
from youtube.ConnectorYouTube import ConnectorYouTube


class ConnectorFactory:
    def createConnector(self, name):
        if name == 'facebook':
            return ConnectorFacebookDiario()
        elif name == 'facebookinsights':
            return ConnectorFacebookInsights()
        elif name == 'facebookposts':
            return ConnectorFacebookPosts()
        elif name == 'instagram':
            return ConnectorInstagram()
        elif name == 'instagramhistorico':
            return ConnectorInstagramHistorico()
        elif name == 'googleadserver':
            return ConnectorGoogleAd()
        elif name == 'googleanalyticsdiario':
            return ConnectorGoogleAnalyticsDiario()
        elif name == 'googleanalyticsmensual':
            return ConnectorGoogleAnalyticsMensual()
        elif name == 'googleanalyticsparcial':
            return ConnectorGoogleAnalyticsParcial()
        elif name == 'twitter':
            return ConnectorTwitter()
        elif name == 'youtube':
            return ConnectorYouTube()
        else:
            return Connector()