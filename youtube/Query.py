import pandas as pd
from mappings.youtube.Queries import TrafficSourceMap,ByCountryMap,ByAgeMap,ByGenderMap,ByDayMap,RevenuesMap,BySubscribedStatusMap,SubscribersMap,ByDeviceMap,ByAdTypeMap,BySharingServiceMap,ByContentTypeMap
from youtube.Service import Service

class Query:
    def __init__(self) -> None:
        self.name = None
        self.metrics = None
        self.dimensions = None
        self.filters = None
        self.sort = None
        self.table = None
        self.ids = None
        self.start_date = None
        self.end_date = None

    def asSQLDF(self) -> pd.DataFrame:
        return self.results

    def asRawDF(self) -> pd.DataFrame:
        return self.results

    def get_results(self,service : Service, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.response = service.resource.reports().query(startDate=start_date,endDate=end_date,
			ids="channel==MINE",filters=self.filters,sort=self.sort,dimensions=self.dimensions,metrics=self.metrics,
            maxResults=200).execute()
        self.results = self.parse_query_response(service.account_name)
    
    def parse_query_response(self, account_name) -> pd.DataFrame:
        cols = []
        data = []
        for i in self.response['columnHeaders']:
            cols.append(i['name'])

        for i in self.response['rows']:
            data.append(i)

        df = pd.DataFrame(data, columns=cols)
        df['Origen'] = account_name
        return df

class TrafficSource(Query):
    def __init__(self) -> None:
        self.name = "Vistas por fuente"
        self.metrics = "views,estimatedMinutesWatched"
        self.dimensions = "insightTrafficSourceType"
        self.filters = None
        self.sort = "-views"
        self.table = None
        self.ids = None
        
    def asSQLDF(self) -> pd.DataFrame:
        return TrafficSourceMap.sql(self.results, self.end_date)

class ByCountry(Query):
    def __init__(self) -> None:
        self.name = "Vistas por pais"
        self.metrics = "views,estimatedMinutesWatched"
        self.dimensions = "country"
        self.filters = None
        self.sort = "-views"
        self.table = None
        self.ids = None
        
    def asSQLDF(self) -> pd.DataFrame:
        return ByCountryMap.sql(self.results,self.end_date)

class ByAge(Query):
    def __init__(self) -> None:
        self.name = "Edad de los espectadores"
        self.metrics = "viewerPercentage"
        self.dimensions = "ageGroup"
        self.filters = None
        self.sort = "ageGroup"
        self.table = None
        self.ids = None
        
    def asSQLDF(self) -> pd.DataFrame:
        return ByAgeMap.sql(self.results, self.end_date)

class ByGender(Query):
    def __init__(self) -> None:
        self.name = "Genero de los espectadores"
        self.metrics = "viewerPercentage"
        self.dimensions = "gender"
        self.filters = None
        self.sort = None
        self.table = None
        self.ids = None
        
    def asSQLDF(self) -> pd.DataFrame:
        return ByGenderMap.sql(self.results, self.end_date)

class ByDay(Query):
    def __init__(self) -> None:
        self.name = "Vistas por dia"
        self.metrics = "views,comments,likes,dislikes,shares,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,subscribersGained,subscribersLost,videosAddedToPlaylists,videosRemovedFromPlaylists,estimatedRevenue,estimatedAdRevenue,grossRevenue,monetizedPlaybacks,playbackBasedCpm,adImpressions,cpm"
        self.dimensions = "day"
        self.filters = None
        self.sort = None
        self.table = None
        self.ids = None
        
    def asSQLDF(self) -> pd.DataFrame:
        return ByDayMap.sql(self.results, self.end_date)

class Revenues(Query):
    def __init__(self) -> None:
        self.name = "Ingresos"
        self.metrics = "estimatedRevenue,estimatedAdRevenue,estimatedRedPartnerRevenue"
        self.dimensions = None
        self.filters = None
        self.sort = None
        self.table = None
        self.ids = None
        
    def asSQLDF(self) -> pd.DataFrame:
        return RevenuesMap.sql(self.results, self.end_date)

class BySubscribedStatus(Query):
    def __init__(self) -> None:
        self.name = "Vistas por estado de suscripcion"
        self.metrics = "views,estimatedMinutesWatched,averageViewDuration"
        self.dimensions = "subscribedStatus"
        self.filters = None
        self.sort = None
        self.table = None
        self.ids = None
        
    def asSQLDF(self) -> pd.DataFrame:
        return BySubscribedStatusMap.sql(self.results, self.end_date)

class Subscribers(Query):
    def __init__(self) -> None:
        self.name = "Flujo de suscriptores por dia"
        self.metrics = "subscribersGained,subscribersLost"
        self.dimensions = "day"
        self.filters = None
        self.sort = None
        self.table = None
        self.ids = None
        
    def asSQLDF(self) -> pd.DataFrame:
        return SubscribersMap.sql(self.results, self.end_date)

class ByDevice(Query):
    def __init__(self) -> None:
        self.name = "Vistas por dispositivo"
        self.metrics = "views,estimatedMinutesWatched"
        self.dimensions = "deviceType"
        self.filters = None
        self.sort = None
        self.table = None
        self.ids = None
        
    def asSQLDF(self) -> pd.DataFrame:
        return ByDeviceMap.sql(self.results, self.end_date)

class ByAdType(Query):
    def __init__(self) -> None:
        self.name = "Ingresos por tipo de anuncio"
        self.metrics = "grossRevenue,adImpressions,cpm"
        self.dimensions = "adType"
        self.filters = None
        self.sort = "-grossRevenue"
        self.table = None
        self.ids = None
        
    def asSQLDF(self) -> pd.DataFrame:
        return ByAdTypeMap.sql(self.results, self.end_date)

class BySharingService(Query):
    def __init__(self) -> None:
        self.name = "Videos compartidos por red social"
        self.metrics = "shares"
        self.dimensions = "sharingService"
        self.filters = None
        self.sort = "-shares"
        self.table = None
        self.ids = None
        
    def asSQLDF(self) -> pd.DataFrame:
        return BySharingServiceMap.sql(self.results, self.end_date)

class ByContentType(Query):
    def __init__(self) -> None:
        self.name = "Vistas por tipo de contenido"
        self.metrics = "views"
        self.dimensions = "liveOrOnDemand"
        self.filters = None
        self.sort = "-views"
        self.table = None
        self.ids = None
        
    def asSQLDF(self) -> pd.DataFrame:
        return ByContentTypeMap.sql(self.results, self.end_date)
