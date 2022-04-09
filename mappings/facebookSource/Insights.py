from datetime import datetime
from mappings.Mapping import Mapping

class FbInsights(Mapping):
	@classmethod
	def clean(cls,data: list[dict]) -> list[dict]:
		retval = [{}]
		retval.pop()
		for i in data:
			dict2Load = {}
			dict2Load['UKEY'] = cls.getUkey(i)
			dict2Load['EndTime'] = datetime.strftime(datetime.strptime(i['end_time'],'%Y-%m-%dT%H:%M:%S+0000'),'%Y-%m-%d')
			dict2Load['Value'] = i['value']
			dict2Load['Target'] = i['page']
			dict2Load['InsightName'] = i['insightName'].upper()
			dict2Load['Period'] = i['period']
			dict2Load['Origen'] = cls.getOrigen(i['page'])
			dict2Load['FechaFiltro'] = datetime.strftime(datetime.strptime(i['end_time'],'%Y-%m-%dT%H:%M:%S+0000'),'%Y-%m-%d')
			dict2Load['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S')
			dict2Load['FechaModificacion'] = None
			retval.append(dict2Load)
		return retval

	@classmethod
	def getUkey(cls,data):
		insightname = data['insightName'].upper()
		endtime = datetime.strftime(datetime.strptime(data['end_time'],'%Y-%m-%dT%H:%M:%S+0000'),'%Y-%m-%d')
		origen = cls.getOrigen(data['page'])
		ukey = origen + insightname + endtime
		return ukey

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
