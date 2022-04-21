from datetime import datetime, timedelta
import urllib.parse
import urllib3
from mappings.Mapping import Mapping
from utils.decode import decode

class PostMapping(Mapping):
	@classmethod
	def raw(cls,data: dict):
		return data

	@classmethod
	def clean(cls,data: dict):
		return data
		
	@classmethod
	def sql(cls,data: dict):
		dict2Load = {}
		dict2Load['UKEY'] = data['id']
		dict2Load['ID'] = data['id']
		dict2Load['Message'] = decode(data, 'message')

		try: 
			dict2Load['Type'] = data['attachments']['data'][0]['type']
		except:
			dict2Load['Type'] = None
		try: 
			dict2Load['Link'] = getUrl(data['attachments'])
		except:
			dict2Load['Link'] = None
		try: 
			dict2Load['Slug'] = getSlug(dict2Load['Link'],dict2Load['Type'])
		except:
			dict2Load['Slug'] = None

		dict2Load['CreatedTime'] = datetime.strftime(datetime.strptime(data['created_time'],'%Y-%m-%dT%H:%M:%S+0000') - timedelta(hours=3),'%Y-%m-%d %H:%M:%S')
		dict2Load['CommentsCount'] = data['comments']
		dict2Load['LikesCount'] = data['likes']
		dict2Load['SharesCount'] = data['shares']
		dict2Load['Origen'] = cls.getOrigen(data['pagename'])
		dict2Load['FechaFiltro'] = datetime.strftime(datetime.strptime(data['created_time'],'%Y-%m-%dT%H:%M:%S+0000') - timedelta(hours=3),'%Y-%m-%d')
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
			
def getUrl(attachments):
	urlTmp = None
	try:
		urlTmp = attachments['data'][0]['url']
		urlTmp = urlTmp.replace('https://l.facebook.com/l.php?u=','')
		urlTmp = urllib.parse.unquote(urlTmp, encoding='utf-8', errors='replace')
		urlTmp = urlTmp.split('&h=', 1)[0]
		urlTmp = urlTmp.replace('http://l.facebook.com/l.php?u=','')

		if urlTmp[:13] == 'http://ow.ly/':
			fp = urllib3.urlopen('http://bit.ly/rgCbf')
			urlTmp = fp.geturl()
	except:
		pass

	return urlTmp

def getSlug(url,type):
	if url[-1] == '/':
		url = url[:-1]
	if type != 'instant_article_legacy' and type != 'share':
		return None
	return url.rsplit('/', 1)[1]