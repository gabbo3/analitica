from datetime import datetime
from mappings.Mapping import Mapping
import pandas as pd
import re

class DFPNoVideo(Mapping):
	@classmethod
	def clean(cls,df: pd.DataFrame):
		data = []
		for row in df.itertuples(index=False,name=None):
			AdsList = {}
			AdUnit = row[1].split('»')
			AdsList['UKEY'] = str(row[0]) + '_' + str(row[1])
			AdsList['Ad_unit'] = None
			try:
				AdsList['Ad_unit1'] = re.sub('\(.*?\)', '', AdUnit[0])
			except:
				AdsList['Ad_unit1'] = None
			try:
				AdsList['Ad_unit2'] = re.sub('\(.*?\)', '', AdUnit[1])
			except:
				AdsList['Ad_unit2'] = None
			try:
				AdsList['Ad_unit3'] = re.sub('\(.*?\)', '', AdUnit[2])
			except:
				AdsList['Ad_unit3'] = None
			try:
				AdsList['Ad_unit4'] = re.sub('\(.*?\)', '', AdUnit[3])
			except:
				AdsList['Ad_unit4'] = None
				
			AdsList['Position_of_pod'] = None
			AdsList['Position_in_pod'] = None
			AdsList['Ad_unit_code'] = row[2]
			AdsList['Ad_unit_reward_amount'] = None
			AdsList['Ad_unit_reward_type'] = None
			AdsList['Total_impressions'] = row[3]
			AdsList['Total_clicks'] = row[4]
			AdsList['fileid'] = None
			AdsList['FechaFiltro'] = row[0]
			AdsList['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
			AdsList['FechaModificacion'] = None
			data.append(AdsList)
		df_ret = pd.DataFrame(data)
		return df_ret