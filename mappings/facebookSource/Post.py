from datetime import datetime
from mappings.Mapping import Mapping
from utils.decode import decode

class PostMongoRaw(Mapping):
	@classmethod
	def clean(cls,data: dict):
		data['pageid'] = data['id'].split('_')[0]
		return data

class PostMongoClean(Mapping):
	@classmethod
	def clean(cls,data: dict):
		return data
		
class PostSQL(Mapping):
	@classmethod
	def clean(cls,data: dict):
		dict2Load = {}
		dict2Load['UKEY'] = data['id']
		dict2Load['ID'] = data['id']
		dict2Load['Message'] = data['message']
		dict2Load['Type'] = decode(data, 'type')
		dict2Load['Link'] = decode(data, 'url')
		dict2Load['Slug'] = decode(data, 'slug')
		dict2Load['CreatedTime'] = datetime.strftime(datetime.strptime(data['created_time'],'%Y-%m-%dT%H:%M:%S+0000'),'%Y-%m-%d %H:%M:%S')
		dict2Load['CommentsCount'] = data['comments']
		dict2Load['LikesCount'] = data['likes']
		dict2Load['SharesCount'] = data['shares']
		dict2Load['Origen'] = cls.getOrigen(data['pagename'])
		dict2Load['FechaFiltro'] = datetime.strftime(datetime.strptime(data['created_time'],'%Y-%m-%dT%H:%M:%S+0000'),'%Y-%m-%d')
		dict2Load['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S')
		dict2Load['FechaModificacion'] = None
		return dict2Load
	
	@classmethod
	def getOrigen(cls,page):
		if page == 'bonelli':
			return 'Bonelli'
		elif page == 'cienradios':
			return 'CienRadios'
		elif page == 'cocina':
			return 'CocinayPlacer'
		elif page == 'coppola':
			return 'Coppola'
		elif page == 'cristinaperez':
			return 'CristinaPerez'
		elif page == 'deportes':
			return 'Deportes'
		elif page == 'dimarco':
			return 'DiMarco'
		elif page == 'experienciatecno':
			return 'ExperienciaTecno'
		elif page == 'fashion':
			return 'Fashion'
		elif page == 'fernandezdiaz':
			return 'FernandezDiaz'
		elif page == 'la100':
			return 'La100'
		elif page == 'libros':
			return 'Libros'
		elif page == 'longobardi':
			return 'Longobardi'
		elif page == 'mia':
			return 'Mia'
		elif page == 'mitreyelcampo':
			return 'MitreCampo'
		elif page == 'motortrend':
			return 'MotorTrend'
		elif page == 'pelado':
			return 'peladolopez'
		elif page == 'planetavivo':
			return 'PlanetaVivo'
		elif page == 'radiomitre':
			return 'Mitre'
		elif page == 'rossi':
			return 'PabloRossi'
		elif page == 'salud':
			return 'Salud360'
		elif page == 'tauro':
			return 'MarcelaTauro'
		elif page == 'todocine':
			return 'TodoCine'
		else:
			return 'ErrorMapeo'
	