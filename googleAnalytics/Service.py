from oauth2client.service_account import ServiceAccountCredentials
from httplib2 import Http
from googleapiclient.discovery import build
import pandas as pd

class Service():
    def __init__(self):
        # Authenticate and create the service for the Core Reporting API
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            'googleAnalytics/GoogleServiceAccount.json', ['https://www.googleapis.com/auth/analytics.readonly'])
        http_auth = credentials.authorize(Http())
        self.resource = build('analytics', 'v3', http=http_auth)