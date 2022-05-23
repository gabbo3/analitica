from datetime import datetime, timedelta
from mappings.Mapping import Mapping
from utils.decode import decode


class StoryMapping(Mapping):
	@classmethod
	def raw(cls,data: dict) -> dict:
		return data

	@classmethod
	def sql(cls,data: dict):
		dict2Load = {}
		dict2Load['UKEY'] = data['id']
		dict2Load['Id'] = data['id']
		# dict2Load['InstagramBusinessAccountId'] = None
		dict2Load['MediaType'] = data['media_type']
		dict2Load['Caption'] = decode(data, 'caption')
		dict2Load['Impressions'] = data['impressions']
		dict2Load['Reach'] = data['reach']
		dict2Load['TapsForward'] = data['taps_forward']
		dict2Load['TapsBack'] = data['taps_back']
		dict2Load['Exits'] = data['exits']
		dict2Load['Replies'] = data['replies']
		dict2Load['Origen'] = cls.getOrigen(data['username'])
		dict2Load['FechaFiltro'] = datetime.strftime(datetime.strptime(data['timestamp'],'%Y-%m-%dT%H:%M:%S+0000')-timedelta(hours=3),'%Y-%m-%d')
		dict2Load['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S')
		dict2Load['FechaModificacion'] = None
		return dict2Load

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