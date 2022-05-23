from datetime import datetime, timedelta
from mappings.Mapping import Mapping


class StatsMapping(Mapping):
	@classmethod
	def raw(cls,data: dict) -> dict:
		id = data['id']
		data['id'] = id + '_' +datetime.strftime(datetime.now(),'%Y-%m-%d')
		return data

	@classmethod
	def sql(cls,data: dict) -> dict:
		retval = {}
		retval['UKEY'] = data['username'] + datetime.strftime(datetime.now(),'%Y-%m-%d')
		retval['UserName'] = data['username']
		retval['FullName'] = data['name']
		retval['ProfilePictureUrl'] = data['profile_picture_url']
		retval['Bio'] = data['biography']
		retval['Website'] = data['website']
		retval['MediaCount'] = data['media_count']
		retval['FollowsCount'] = data['follows_count']
		retval['FollowersCount'] = data['followers_count']
		retval['UserName'] = data['username']
		retval['FechaFiltro'] = datetime.strftime(datetime.now(),'%Y-%m-%d')
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S')
		retval['FechaModificacion'] = None
		return retval
