from datetime import datetime
import pandas as pd
from mappings.Mapping import Mapping

class YTVideosMap(Mapping):
	@classmethod
	def sql(cls,df : pd.DataFrame):
		retval = pd.DataFrame()
		retval['UKEY'] = df['id']
		retval['Id'] = df['id']
		retval['PublishedAt'] = pd.to_datetime(df['snippet.publishedAt']).dt.strftime('%Y-%m-%d %H:%M:%S.000')
		retval['Title'] = df['snippet.title']
		retval['Description'] = df['snippet.description']
		retval['ChannelTitle'] = df['snippet.channelTitle']
		retval['Tags'] = str(df['snippet.tags'])
		retval['CategoryId'] = df['snippet.categoryId']
		retval['LiveBroadcastContent'] = df['snippet.liveBroadcastContent']
		retval['ViewCount'] = df['statistics.viewCount']
		retval['LikeCount'] = df['statistics.likeCount']
		retval['DislikeCount'] = df['statistics.dislikeCount']
		retval['FavoriteCount'] = df['statistics.favoriteCount']
		retval['CommentCount'] = df['statistics.commentCount']
		retval['Duration'] = [str(pd.Timedelta(x)).split('days')[1] for x in df['contentDetails.duration']]
		retval['Caption'] = df['contentDetails.caption']
		retval['LicensedContent'] = df['contentDetails.licensedContent']
		retval['HasCustomThumbnail'] = df['contentDetails.hasCustomThumbnail']
		retval['Origen'] = [cls.getOrigen(x) for x in df['snippet.channelTitle']]
		retval['FechaFiltro'] = pd.to_datetime(df['snippet.publishedAt']).dt.strftime('%Y-%m-%d')
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

	@classmethod
	def getOrigen(cls, channel_name):
		if channel_name == 'La 100':
			return 'La100'
		else:
			return 'Mitre'