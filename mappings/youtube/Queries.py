from datetime import datetime
import pandas as pd
from mappings.Mapping import Mapping

class TrafficSourceMap(Mapping):
	def sql(data : pd.DataFrame, end_date):
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + data['Origen'] + data['insightTrafficSourceType']
		retval['InsightTrafficSourceType'] = data['insightTrafficSourceType']
		retval['Views'] = data['views']
		retval['EstimatedMinutesWatched'] = data['estimatedMinutesWatched']
		retval['Origen'] = data['Origen']
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

class ByCountryMap(Mapping):
	def sql(data : pd.DataFrame, end_date):
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + data['Origen'] + data['country']
		retval['Country'] = data['country']
		retval['Views'] = data['views']
		retval['EstimatedMinutesWatched'] = data['estimatedMinutesWatched']
		retval['Origen'] = data['Origen']
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

class ByAgeMap(Mapping):
	def sql(data : pd.DataFrame, end_date):
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + data['Origen'] + data['ageGroup']
		retval['AgeGroup'] = data['ageGroup']
		retval['ViewerPercentage'] = data['viewerPercentage']
		retval['Origen'] = data['Origen']
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

class ByGenderMap(Mapping):
	def sql(data : pd.DataFrame, end_date):
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + data['Origen'] + data['gender']
		retval['Gender'] = data['gender']
		retval['ViewerPercentage'] = data['viewerPercentage']
		retval['Origen'] = data['Origen']
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

class ByDayMap(Mapping):
	def sql(data : pd.DataFrame, end_date):
		retval = pd.DataFrame()
		retval['UKEY'] = data['day'] + data['Origen']
		retval['Day'] = data['day']
		retval['Views'] = data['views']
		retval['Comments'] = data['comments']
		retval['Likes'] = data['likes']
		retval['Dislikes'] = data['dislikes']
		retval['Shares'] = data['shares']
		retval['EstimatedMinutesWatched'] = data['estimatedMinutesWatched']
		retval['AverageViewDuration'] = data['averageViewDuration']
		retval['AverageViewPercentage'] = data['averageViewPercentage']
		retval['SubscribersGained'] = data['subscribersGained']
		retval['SubscribersLost'] = data['subscribersLost']
		retval['VideosAddedToPlaylists'] = data['videosAddedToPlaylists']
		retval['VideosRemovedFromPlaylists'] = data['videosRemovedFromPlaylists']
		retval['EstimatedRevenue'] = data['estimatedRevenue']
		retval['EstimatedAdRevenue'] = data['estimatedAdRevenue']
		retval['GrossRevenue'] = data['grossRevenue']
		retval['MonetizedPlaybacks'] = data['monetizedPlaybacks']
		retval['PlaybackBasedCpm'] = data['playbackBasedCpm']
		retval['AdImpressions'] = data['adImpressions']
		retval['Cpm'] = data['cpm']
		retval['Origen'] = data['Origen']
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

class RevenuesMap(Mapping):
	def sql(data : pd.DataFrame, end_date):
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + data['Origen']
		retval['EstimatedRevenue'] = data['estimatedRevenue']
		retval['EstimatedAdRevenue'] = data['estimatedAdRevenue']
		retval['EstimatedRedPartnerRevenue'] = data['estimatedRedPartnerRevenue']
		retval['Origen'] = data['Origen']
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

class BySubscribedStatusMap(Mapping):
	def sql(data : pd.DataFrame, end_date):
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + data['Origen'] + data['subscribedStatus']
		retval['SubscribedStatus'] = data['subscribedStatus']
		retval['Views'] = data['views']
		retval['EstimatedMinutesWatched'] = data['estimatedMinutesWatched']
		retval['AverageViewDuration'] = data['averageViewDuration']
		retval['Origen'] = data['Origen']
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

class SubscribersMap(Mapping):
	def sql(data : pd.DataFrame, end_date):
		retval = pd.DataFrame()
		retval['UKEY'] = data['day'] + data['Origen']
		retval['Day'] = data['day']
		retval['SubscribersGained'] = data['subscribersGained']
		retval['SubscribersLost'] = data['subscribersLost']
		retval['Origen'] = data['Origen']
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
				
		return retval

class ByDeviceMap(Mapping):
	def sql(data : pd.DataFrame, end_date):
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + data['Origen'] + data['deviceType']
		retval['DeviceType'] = data['deviceType']
		retval['Views'] = data['views']
		retval['EstimatedMinutesWatched'] = data['estimatedMinutesWatched']
		retval['Origen'] = data['Origen']
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

class ByAdTypeMap(Mapping):
	def sql(data : pd.DataFrame, end_date):
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + data['Origen'] + data['adType']
		retval['AdType'] = data['adType']
		retval['GrossRevenue'] = data['grossRevenue']
		retval['AdImpressions'] = data['adImpressions']
		retval['Cpm'] = data['cpm']
		retval['Origen'] = data['Origen']
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

class BySharingServiceMap(Mapping):
	def sql(data : pd.DataFrame, end_date):
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + data['Origen'] + data['sharingService']
		retval['SharingService'] = data['sharingService']
		retval['Shares'] = data['shares']
		retval['Origen'] = data['Origen']
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

class ByContentTypeMap(Mapping):
	def sql(data : pd.DataFrame, end_date):
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + data['Origen'] + data['liveOrOnDemand']
		retval['liveOrOnDemand'] = data['liveOrOnDemand']
		retval['views'] = data['views']
		retval['Origen'] = data['Origen']
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval
