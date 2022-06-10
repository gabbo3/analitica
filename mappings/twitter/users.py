from datetime import datetime, timedelta
import pandas as pd
from mappings.Mapping import Mapping

class UsersMap(Mapping):
	@classmethod
	def sql(cls, data : pd.DataFrame):
		retval = pd.DataFrame()
		retval['UKEY'] = data['id_str'] + datetime.strftime(datetime.now(),'%Y-%m-%d')
		retval['Id'] = data['id']
		retval['Name'] = data['name']
		retval['CreatedAt'] = [datetime.strftime(datetime.strptime(x,'%a %b %d %H:%M:%S +0000 %Y')-timedelta(hours=3),'%Y-%m-%d %H:%M:%S') for x in data['created_at']]
		retval['FriendsCount'] = data['friends_count']
		retval['FollowersCount'] = data['followers_count']
		retval['FavouritesCount'] = data['favourites_count']
		retval['StatusesCount'] = data['statuses_count']
		retval['Following'] = data['following']
		retval['ListedCount'] = data['listed_count']
		retval['Origen'] = data['screen_name']
		retval['FechaFiltro'] = datetime.strftime(datetime.now(),'%Y-%m-%d')
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S')
		retval['FechaModificacion'] = None
		return retval