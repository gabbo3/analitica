from datetime import datetime, timedelta
from sqlite3 import Timestamp

import pandas as pd
from mappings.Mapping import Mapping
from utils.decode import decode


class InsightMapping(Mapping):
	@classmethod
	def raw_daily(cls,data: dict) -> pd.DataFrame:
		array = []
		for i in data['values']:
			IGList = {}
			IGList['name'] = data['name']
			IGList['period'] = data['period']
			IGList['title'] = data['title']
			IGList['description'] = data['description']
			IGList['value'] = i['value']
			IGList['end_time'] = i['end_time']
			IGList['id'] = data['id'] + '_' + i['end_time']
			array.append(IGList)
		df_ret = pd.DataFrame(array)
		return df_ret

	@classmethod
	def raw_lifetime(cls,data: dict) -> pd.DataFrame:
		array = []
		for i in data['values']:
			for val in i['value']:
				IGList = {}
				IGList['name'] = data['name']
				IGList['period'] = data['period']
				IGList['title'] = data['title']
				IGList['description'] = data['description']
				IGList['dimension'] = val
				IGList['value'] = i['value'][val]
				IGList['end_time'] = i['end_time']
				IGList['id'] = data['id'] + '_' + val + '_' + i['end_time']
				array.append(IGList)
		df_ret = pd.DataFrame(array)
		return df_ret

	@classmethod
	def sql_lifetime(cls,data: dict) -> pd.DataFrame:
		array = []
		for i in data['values']:
			for val in i['value']:
				IGList = {}
				timestamp = datetime.strftime(datetime.strptime(i['end_time'],'%Y-%m-%dT%H:%M:%S+0000') - timedelta(hours=3),'%Y-%m-%d')
				IGList['UKEY'] = timestamp + data['title'] + str(val)
				IGList['Date'] = timestamp
				IGList['AudienceType'] = data['title']
				IGList['AudienceGroup'] = val
				IGList['TotalAudience'] = i['value'][val]
				IGList['Description'] = data['description']
				IGList['Origen'] = None
				IGList['FechaFiltro'] = timestamp
				IGList['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S')
				IGList['FechaModificacion'] = None
				array.append(IGList)
		df_ret = pd.DataFrame(array)
		return df_ret

	@classmethod
	def sql_daily(cls,data: dict) -> pd.DataFrame:
		array = []
		for i in data['values']:
			IGList = {}
			IGList['UKEY'] = data['name']
			IGList['Id'] = data['name']
			
			# IGList['UKEY'] = 
			# IGList['EndTime'] = 
			# IGList['Impressions'] = 
			# IGList['Reach'] = 
			# IGList['ProfileViews'] = 
			# IGList['FollowerCount'] = 
			# IGList['EmailContacts'] = 
			# IGList['PhoneCallClicks'] = 
			# IGList['TextMessageClicks'] = 
			# IGList['GetDirectionsClicks'] = 
			# IGList['Websiteclicks'] = 
			# IGList['Period'] = 
			# IGList['Origen'] = 
			# IGList['FechaFiltro'] = 
			# IGList['FechaCreacion'] = 
			# IGList['FechaModificacion'] = 

			IGList['period'] = data['period']
			IGList['title'] = data['title']
			IGList['description'] = data['description']
			IGList['value'] = i['value']
			IGList['end_time'] = i['end_time']
			IGList['id'] = data['id'] + '_' + i['end_time']
			array.append(IGList)
		df_ret = pd.DataFrame(array)
		return df_ret

	@classmethod
	def getUkey(cls,data):
		id = data['id']
		origen = cls.getOrigen(data['username'])
		ukey = origen + str(id)
		return ukey

	@classmethod
	def getOrigen(cls,username):
		if username == 'la100fm':
			return 'La100'
		elif username == 'bonelliok':
			return 'Bonelli'
		elif username == 'cienradios':
			return 'Cienradios'
		elif username == 'fashionclickok':
			return 'Fashion'
		elif username == 'fernandezdiazok':
			return 'FernandezD'
		elif username == 'longobardioficial':
			return 'Longobardi'
		elif username == 'miafmok':
			return 'Mia'
		elif username == 'radiomitre':
			return 'Mitre'
		else:
			return 'ErrorMapeo'