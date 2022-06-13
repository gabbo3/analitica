from datetime import datetime
import logging
import pandas as pd
from connectors.Connector import Connector
from mappings.youtube.Videos import YTVideosMap
from youtube.Service import Service
from youtube.Query import Query,TrafficSource,ByCountry,ByAge,ByGender,ByDay,Revenues,BySubscribedStatus,Subscribers,ByDevice,ByAdType,BySharingService,ByContentType
from databases.MongoDB.MongoDB import MongoDB
from databases.SQLServer.SQLServer import SQLServer
from dateutil.relativedelta import relativedelta

class ConnectorYouTube(Connector):
	def __init__(self) -> None:
		self.mongo = MongoDB()
		self.sql = SQLServer()
		self.set_dates()
		self.analyticsServices = self.loadAnalyticsServices()
		self.dataServices = self.loadDataServices()
		self.queries = self.loadQueries()

	def execute(self):
		for analyticService in self.analyticsServices:
			logging.info("Ejecutando :" + analyticService.account_name)
			dict2load = {}
			dict2load['id'] = self.end_date
			dict2load['Results'] = {}
			for q in self.queries:
				dict2load['Results'][q.name] = []
			for q in self.queries:
				q.get_results(analyticService,self.start_date,self.end_date)
				logging.info(q.name + " OK")
				self.sql.upsert(q.asSQLDF(), 'PY_YT' + q.table)
				# dict2load['Results'][q.name].append(json.loads(q.asRawDF().to_json(orient='records')))
			# self.mongo.upsertDict(dict2load, 'RAWDATA', 'GoogleAnalyticsDiario')
		
		for dataService in self.dataServices:
			logging.info("Ejecutando :" + dataService.account_name)
			df_videos = self.get_videos(dataService)
			logging.info("get_videos: OK")
			self.sql.upsert(YTVideosMap.sql(df_videos),'PY_YT_Videos')


	def get_videos(self, dataService : Service) -> pd.DataFrame:
		channels_response = dataService.resource.channels().list(mine=True,part="contentDetails").execute()

		for channel in channels_response["items"]:
			uploads_list_id = channel["contentDetails"]["relatedPlaylists"]["uploads"]

		videos = []
		request = dataService.resource.playlistItems().list(part="contentDetails",playlistId=uploads_list_id,maxResults=4500)
		while request:
			response = request.execute()
			for i in response['items']:
				videos.append(i['contentDetails']['videoId'])
			request = dataService.resource.playlistItems().list_next(request,response)

		videosClean = list(set(videos))
		stats = []
		for videoId in videosClean:
			response = dataService.resource.videos().list(part="id, snippet, contentDetails, statistics",id=videoId).execute()
			try:
				stats.append(response['items'][0])
			except:
				print(response)

		return pd.json_normalize(stats)

	def set_dates(self):
		# 1 del mes
		self.start_date = format(datetime.today().replace(day=1) - relativedelta(months=1), '%Y-%m-%d')
		# ultimo dia del mes
		self.end_date = format(datetime.today() - relativedelta(months=0,days=datetime.today().day), '%Y-%m-%d')

	def loadQueries(self):
		retval = list[Query]()
		retval.append(TrafficSource())
		retval.append(ByCountry())
		retval.append(ByAge())
		retval.append(ByGender())
		retval.append(ByDay())
		retval.append(Revenues())
		retval.append(BySubscribedStatus())
		retval.append(Subscribers())
		retval.append(ByDevice())
		retval.append(ByAdType())
		retval.append(BySharingService())
		retval.append(ByContentType())
		return retval

	def loadAnalyticsServices(self):
		logging.info("Validando acceso : La100-Analytics")
		self.La100Analytics = Service("La100-Analytics","youtubeAnalytics","v2",
			"La100","youtube/client_secrets.json",
			"youtube/credentials/La100-Analytics.json",
			["https://www.googleapis.com/auth/yt-analytics.readonly"
			,"https://www.googleapis.com/auth/yt-analytics-monetary.readonly"
			,"https://www.googleapis.com/auth/youtube"
			,"https://www.googleapis.com/auth/youtubepartner"])

		logging.info("Validando acceso : Mitre-Analytics")
		self.MitreAnalytics = Service("Mitre-Analytics","youtubeAnalytics","v2",
			"Mitre","youtube/client_secrets.json",
			"youtube/credentials/Mitre-Analytics.json",
			["https://www.googleapis.com/auth/yt-analytics.readonly"
			,"https://www.googleapis.com/auth/yt-analytics-monetary.readonly"
			,"https://www.googleapis.com/auth/youtube"
			,"https://www.googleapis.com/auth/youtubepartner"])

		return [self.La100Analytics, self.MitreAnalytics]
		# return [self.La100Analytics]
		# return [self.MitreAnalytics]

	def loadDataServices(self):
		logging.info("Validando acceso : La100-Data")
		self.La100Data = Service("La100-Data","youtube","v3",
			"La100","youtube/client_secrets.json",
			"youtube/credentials/La100-Data.json",
			["https://www.googleapis.com/auth/youtube",
			"https://www.googleapis.com/auth/youtube.force-ssl",
			"https://www.googleapis.com/auth/youtube.readonly",
			"https://www.googleapis.com/auth/youtubepartner"])

		logging.info("Validando acceso : Mitre-Data")
		self.MitreData = Service("Mitre-Data","youtube","v3",
			"Mitre","youtube/client_secrets.json",
			"youtube/credentials/Mitre-Data.json",
			["https://www.googleapis.com/auth/youtube",
			"https://www.googleapis.com/auth/youtube.force-ssl",
			"https://www.googleapis.com/auth/youtube.readonly",
			"https://www.googleapis.com/auth/youtubepartner"])

		return [self.La100Data, self.MitreData]
		# return [self.La100Data]
		# return [self.MitreData]