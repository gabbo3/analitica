from datetime import datetime, timedelta
import pandas as pd
from mappings.Mapping import Mapping
from utils.decode import decode

class TweetMap(Mapping):
	@classmethod
	def sql(cls, data : pd.DataFrame):
		retval = pd.DataFrame()
		retval['UKEY'] = data['id']
		retval['Id'] = data['id']
		retval['CreatedAt'] = [datetime.strftime(datetime.strptime(x,'%a %b %d %H:%M:%S +0000 %Y')-timedelta(hours=3),'%Y-%m-%d %H:%M:%S') for x in data['created_at']]
		retval['FullText'] = data['full_text']
		retval['RetweetCount'] = data['retweet_count']
		retval['FavoriteCount'] = data['favorite_count']
		retval['PossiblySensitive'] = decode(data,'possibly_sensitive','False')
		retval['Lang'] = data['lang']
		retval['Hashtags'] = [cls.cleanHashtag(x) for x in data['entities.hashtags']]
		retval['UserMentions'] = [cls.cleanMentionedUser(x) for x in data['entities.user_mentions']]
		retval['URLs'] = [cls.cleanURL(x) for x in data['entities.urls']]
		retval['Origen'] = [cls.getOrigen(x) for x in data['user.screen_name']]
		retval['FechaFiltro'] = [datetime.strftime(datetime.strptime(x,'%a %b %d %H:%M:%S +0000 %Y')-timedelta(hours=3),'%Y-%m-%d') for x in data['created_at']]
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S')
		retval['FechaModificacion'] = None
		return retval

	@classmethod
	def cleanHashtag(cls, data : list) -> str:
		retval = str()
		for k in data:
			retval += k['text']
			retval += ','
		retval = retval[:-1]
		return retval

	@classmethod
	def cleanMentionedUser(cls, data : list) -> str:
		retval = str()
		for k in data:
			retval += k['screen_name']
			retval += ','
		retval = retval[:-1]
		return retval

	@classmethod
	def cleanURL(cls, data : list) -> str:
		retval = str()
		for k in data:
			retval += k['expanded_url']
			retval += ','
		retval = retval[:-1]
		return retval

	@classmethod
	def getOrigen(cls, user_name) -> str:
		if user_name == 'la100fm':
			return 'La100'
		else:
			return 'Mitre'

	