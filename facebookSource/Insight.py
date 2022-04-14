from datetime import datetime, timedelta
import pandas as pd
import requests
import json
import logging

from mappings.facebookSource.Insights import InsightMapping


class Insight:
	def __init__(self,name,pageid,token,pagename,n_days=10,since:str = None):
		self.name = name
		self.pageid = pageid
		self.token = token
		self.pagename = pagename
		self.data = self.loadData(n_days,since)

	def loadData(self,n_days=10,since:str = None):
		if since:
			since = datetime.strptime(since,'%Y-%m-%d')
			if datetime.now()-timedelta(days=365*2) > since :
				since = datetime.now()-timedelta(days=365*2)
			# log this:
			# print('since: ', since)
		else:
			since = datetime.now().replace(hour=0,minute=0,second=0,microsecond=0)-timedelta(days=n_days)
			# log this:
			# print('n_days: ',n_days)
		
		until = since + timedelta(days=n_days)

		url = 'https://graph.facebook.com/'
		url += str(self.pageid)
		url += '/insights'
		url += '?access_token='
		url += self.token
		url +='&metric='
		url += self.name
		url +='&period=day'
		url +='&since='
		url += str(since)
		url +='&until='
		url += str(until)

		retval = [{}]
		retval.pop()

		while url:
			r = requests.get(url)
			response = json.loads(r.text)
			try:
				array = []
				for i in response['data'][0]['values']:
					fbList = {}
					id = self.pagename + '_' + self.name + '_' + i['end_time']
					fbList['id'] = id
					fbList['end_time'] = i['end_time']
					fbList['insightName'] = self.name
					fbList['page'] = self.pagename
					fbList['pageid'] = self.pageid
					fbList['period'] = response['data'][0]['period']
					fbList['value'] = i['value']
					array.append(fbList)
			except Exception as e:
				logging.warning('Insight name: ' + self.name)
				logging.warning(e, exc_info=True)
			retval += array
			# log this:
			try:
				url = response['paging']['next']
			except:
				url = None
		return retval

	def asRawDF(self):
		return pd.DataFrame(InsightMapping.raw(self.data))

	def asCleanDF(self):
		return pd.DataFrame(InsightMapping.clean(self.data))

	def asSQLDF(self):
		return pd.DataFrame(InsightMapping.sql(self.data))
