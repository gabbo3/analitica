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
    
    def query(self, ids, start_date, end_date, metrics, dimensions,filters=None) -> pd.DataFrame:
        self.response = self.resource.data().ga().get(ids=ids,start_date=start_date, end_date=end_date,
            metrics=metrics, dimensions=dimensions, start_index='1', max_results='1000',filters=filters).execute()
        return self.parse_query_response()

    def parse_query_response(self) -> pd.DataFrame:
        cols = []
        data = []
        for i in self.response['columnHeaders']:
            cols.append(i['name'][3:])

        for i in self.response['rows']:
            data.append(i)

        df = pd.DataFrame(data, columns=cols)
        df['Origen'] = self.response['profileInfo']['profileName']
        
        return df