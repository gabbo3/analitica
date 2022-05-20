from datetime import datetime
import pandas as pd
from mappings.Mapping import Mapping

class TraficoTotalMap(Mapping):
	def sql(data : pd.DataFrame, end_date : str)-> pd.DataFrame:
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + data['Origen']
		retval['Users'] = data['users']
		retval['Sessions'] = data['sessions']
		retval['Pageviews'] = data['pageviews']
		retval['PromedioSesión'] = data['avgSessionDuration']
		retval['PorcentajeRebote'] = data['bounceRate']
		retval['Origen'] = data['Origen']
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

class TraficoXCanalMap(Mapping):
	def sql(data : pd.DataFrame, end_date : str)-> pd.DataFrame:
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + data['Origen'] + data['channelGrouping']
		retval['Users'] = data['users']
		retval['Sessions'] = data['sessions']
		retval['Pageviews'] = data['pageviews']
		retval['PromedioSesión'] = data['avgSessionDuration']
		retval['PorcentajeRebote'] = data['bounceRate']
		retval['ChannelGrouping'] = data['channelGrouping']
		retval['Origen'] = data['Origen']
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

class TraficoXPaisMap(Mapping):
	def sql(data : pd.DataFrame, end_date : str)-> pd.DataFrame:
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + data['Origen'] + data['country']
		retval['Users'] = data['users']
		retval['Sessions'] = data['sessions']
		retval['Pageviews'] = data['pageviews']
		retval['PromedioSesión'] = data['avgSessionDuration']
		retval['PorcentajeRebote'] = data['bounceRate']
		retval['Pais'] = data['country']
		retval['Origen'] = data['Origen']
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

class TraficoXPaisDispositivoMap(Mapping):
	def sql(data : pd.DataFrame, end_date : str)-> pd.DataFrame:
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + data['Origen'] + data['country'] + data['deviceCategory']
		retval['Users'] = data['users']
		retval['Sessions'] = data['sessions']
		retval['Pageviews'] = data['pageviews']
		retval['PromedioSesión'] = data['avgSessionDuration']
		retval['PorcentajeRebote'] = data['bounceRate']
		retval['Pais'] = data['country']
		retval['DeviceCategory'] = data['deviceCategory']
		retval['Origen'] = data['Origen']
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

class TraficoXDispositivoMap(Mapping):
	def sql(data : pd.DataFrame, end_date : str)-> pd.DataFrame:
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + data['Origen'] + data['deviceCategory']
		retval['Users'] = data['users']
		retval['Sessions'] = data['sessions']
		retval['Pageviews'] = data['pageviews']
		retval['PromedioSesión'] = data['avgSessionDuration']
		retval['PorcentajeRebote'] = data['bounceRate']
		retval['DeviceCategory'] = data['deviceCategory']
		retval['Origen'] = data['Origen']
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

class TraficoXFuenteMap(Mapping):
	def sql(data : pd.DataFrame, end_date : str)-> pd.DataFrame:
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + data['Origen'] + data['socialNetwork']
		retval['Users'] = data['users']
		retval['Sessions'] = data['sessions']
		retval['Pageviews'] = data['pageviews']
		retval['PromedioSesión'] = data['avgSessionDuration']
		retval['PorcentajeRebote'] = data['bounceRate']
		retval['Fuente'] = data['socialNetwork']
		retval['Origen'] = data['Origen']
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

class TraficoTotalXRRSSMap(Mapping):
	def sql(data : pd.DataFrame, end_date : str)-> pd.DataFrame:
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + data['Origen']
		retval['Users'] = data['users']
		retval['Sessions'] = data['sessions']
		retval['Pageviews'] = data['pageviews']
		retval['PromedioSesión'] = data['avgSessionDuration']
		retval['PorcentajeRebote'] = data['bounceRate']
		retval['Origen'] = data['Origen']
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

class TraficoPlayerLa100AlternativoMap(Mapping):
	def sql(data : pd.DataFrame, end_date : str)-> pd.DataFrame:
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + data['Origen'] + 'TODOSSINLA100'
		retval['Users'] = data['users']
		retval['Sessions'] = data['sessions']
		retval['Pageviews'] = data['pageviews']
		retval['PromedioSesión'] = data['avgSessionDuration']
		retval['PorcentajeRebote'] = data['bounceRate']
		retval['pagePath'] = 'TODOSSINLA100'
		retval['Origen'] = data['Origen']
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

class TraficoPlayersLa100XPlayerMap(Mapping):
	def sql(data : pd.DataFrame, end_date : str)-> pd.DataFrame:
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + data['Origen'] + data['pagePath']
		retval['Users'] = data['users']
		retval['Sessions'] = data['sessions']
		retval['Pageviews'] = data['pageviews']
		retval['PromedioSesión'] = data['avgSessionDuration']
		retval['PorcentajeRebote'] = data['bounceRate']
		retval['pagePath'] = data['pagePath']
		retval['Origen'] = data['Origen']
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

class TraficoTotalPlayersLa100Map(Mapping):
	def sql(data : pd.DataFrame, end_date : str)-> pd.DataFrame:
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + data['Origen'] + 'TODOS'
		retval['Users'] = data['users']
		retval['Sessions'] = data['sessions']
		retval['Pageviews'] = data['pageviews']
		retval['PromedioSesión'] = data['avgSessionDuration']
		retval['PorcentajeRebote'] = data['bounceRate']
		retval['pagePath'] = 'TODOS'
		retval['Origen'] = data['Origen']
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

class TraficoXHostnameMap(Mapping):
	def sql(data : pd.DataFrame, end_date : str)-> pd.DataFrame:
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + data['Origen'] + data['hostname']
		retval['Users'] = data['users']
		retval['Sessions'] = data['sessions']
		retval['Pageviews'] = data['pageviews']
		retval['PromedioSesión'] = data['avgSessionDuration']
		retval['PorcentajeRebote'] = data['bounceRate']
		retval['Hostname'] = data['hostname']
		retval['Origen'] = data['Origen']
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

class TraficoXHostnameADCMap(Mapping):
	def sql(data : pd.DataFrame, end_date : str)-> pd.DataFrame:
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + data['Origen'] + 'TOTALADC'
		retval['Users'] = data['users']
		retval['Sessions'] = data['sessions']
		retval['Pageviews'] = data['pageviews']
		retval['PromedioSesión'] = data['avgSessionDuration']
		retval['PorcentajeRebote'] = data['bounceRate']
		retval['Hostname'] = 'TOTALADC'
		retval['Origen'] = data['Origen']
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

class TraficoXHostnameVerticalMap(Mapping):
	def sql(data : pd.DataFrame, end_date : str)-> pd.DataFrame:
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + data['Origen'] + 'TOTALVERTICALES'
		retval['Users'] = data['users']
		retval['Sessions'] = data['sessions']
		retval['Pageviews'] = data['pageviews']
		retval['PromedioSesión'] = data['avgSessionDuration']
		retval['PorcentajeRebote'] = data['bounceRate']
		retval['Hostname'] = 'TOTALVERTICALES'
		retval['Origen'] = data['Origen']
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval
