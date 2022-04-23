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
				IGList['Origen'] = cls.getOrigen(data['id'].split('/')[0])
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
			timestamp = datetime.strftime(datetime.strptime(i['end_time'],'%Y-%m-%dT%H:%M:%S+0000') - timedelta(hours=3),'%Y-%m-%d')
			IGList['UKEY'] = data['id'] + '_' + timestamp
			IGList['EndTime'] = timestamp
			IGList['InsightName'] = data['name']
			IGList['Value'] = i['value']
			IGList['Period'] = data['period']
			IGList['Origen'] = cls.getOrigen(data['id'].split('/')[0])
			IGList['FechaFiltro'] = timestamp
			IGList['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S')
			IGList['FechaModificacion'] = None
			array.append(IGList)
		df_ret = pd.DataFrame(array)
		return df_ret

	@classmethod
	def getOrigen(cls,username):
		if username == '17841400247488610':
			return 'La100'
		elif username == '17841405767270271':
			return 'Bonelli'
		elif username == '17841400005451507':
			return 'Cienradios'
		elif username == '17841401334624038':
			return 'Fashion'
		elif username == '17841405997846545':
			return 'FernandezD'
		elif username == '17841405322953987':
			return 'Longobardi'
		elif username == '17841405609333285':
			return 'Mia'
		elif username == '17841401850931197':
			return 'Mitre'
		else:
			return 'ErrorMapeo'