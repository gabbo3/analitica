from datetime import datetime
import pandas as pd
from mappings.Mapping import Mapping

class TraficoTotalMap(Mapping):
	def sql(data : pd.DataFrame, end_date : str)-> pd.DataFrame:
		retval = pd.DataFrame()
		origen = getOrigen(data['Origen'])
		retval['UKEY'] = end_date + origen
		retval['Users'] = data['users']
		retval['Sessions'] = data['sessions']
		retval['Pageviews'] = data['pageviews']
		retval['PromedioSesión'] = data['avgSessionDuration']
		retval['PorcentajeRebote'] = data['bounceRate']
		retval['Origen'] = getOrigen(data['Origen'])
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

class TraficoXCanalMap(Mapping):
	def sql(data : pd.DataFrame, end_date : str)-> pd.DataFrame:
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + getOrigen(data['Origen']) + data['channelGrouping']
		retval['Users'] = data['users']
		retval['Sessions'] = data['sessions']
		retval['Pageviews'] = data['pageviews']
		retval['PromedioSesión'] = data['avgSessionDuration']
		retval['PorcentajeRebote'] = data['bounceRate']
		retval['ChannelGrouping'] = data['channelGrouping']
		retval['Origen'] = getOrigen(data['Origen'])
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

class TraficoXPaisMap(Mapping):
	def sql(data : pd.DataFrame, end_date : str)-> pd.DataFrame:
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + getOrigen(data['Origen']) + data['country']
		retval['Users'] = data['users']
		retval['Sessions'] = data['sessions']
		retval['Pageviews'] = data['pageviews']
		retval['PromedioSesión'] = data['avgSessionDuration']
		retval['PorcentajeRebote'] = data['bounceRate']
		retval['Pais'] = data['country']
		retval['Origen'] = getOrigen(data['Origen'])
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

class TraficoXPaisDispositivoMap(Mapping):
	def sql(data : pd.DataFrame, end_date : str)-> pd.DataFrame:
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + getOrigen(data['Origen']) + data['country'] + data['deviceCategory']
		retval['Users'] = data['users']
		retval['Sessions'] = data['sessions']
		retval['Pageviews'] = data['pageviews']
		retval['PromedioSesión'] = data['avgSessionDuration']
		retval['PorcentajeRebote'] = data['bounceRate']
		retval['Pais'] = data['country']
		retval['DeviceCategory'] = data['deviceCategory']
		retval['Origen'] = getOrigen(data['Origen'])
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

class TraficoXDispositivoMap(Mapping):
	def sql(data : pd.DataFrame, end_date : str)-> pd.DataFrame:
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + getOrigen(data['Origen']) + data['deviceCategory']
		retval['Users'] = data['users']
		retval['Sessions'] = data['sessions']
		retval['Pageviews'] = data['pageviews']
		retval['PromedioSesión'] = data['avgSessionDuration']
		retval['PorcentajeRebote'] = data['bounceRate']
		retval['DeviceCategory'] = data['deviceCategory']
		retval['Origen'] = getOrigen(data['Origen'])
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

class TraficoXFuenteMap(Mapping):
	def sql(data : pd.DataFrame, end_date : str)-> pd.DataFrame:
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + getOrigen(data['Origen']) + data['socialNetwork']
		retval['Users'] = data['users']
		retval['Sessions'] = data['sessions']
		retval['Pageviews'] = data['pageviews']
		retval['PromedioSesión'] = data['avgSessionDuration']
		retval['PorcentajeRebote'] = data['bounceRate']
		retval['Fuente'] = data['socialNetwork']
		retval['Origen'] = getOrigen(data['Origen'])
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

class TraficoTotalXRRSSMap(Mapping):
	def sql(data : pd.DataFrame, end_date : str)-> pd.DataFrame:
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + getOrigen(data['Origen'])
		retval['Users'] = data['users']
		retval['Sessions'] = data['sessions']
		retval['Pageviews'] = data['pageviews']
		retval['PromedioSesión'] = data['avgSessionDuration']
		retval['PorcentajeRebote'] = data['bounceRate']
		retval['Origen'] = getOrigen(data['Origen'])
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

class TraficoPlayerLa100AlternativoMap(Mapping):
	def sql(data : pd.DataFrame, end_date : str)-> pd.DataFrame:
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + getOrigen(data['Origen']) + 'TODOSSINLA100'
		retval['Users'] = data['users']
		retval['Sessions'] = data['sessions']
		retval['Pageviews'] = data['pageviews']
		retval['PromedioSesión'] = data['avgSessionDuration']
		retval['PorcentajeRebote'] = data['bounceRate']
		retval['pagePath'] = 'TODOSSINLA100'
		retval['Origen'] = getOrigen(data['Origen'])
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

class TraficoPlayersLa100XPlayerMap(Mapping):
	def sql(data : pd.DataFrame, end_date : str)-> pd.DataFrame:
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + getOrigen(data['Origen']) + data['pagePath']
		retval['Users'] = data['users']
		retval['Sessions'] = data['sessions']
		retval['Pageviews'] = data['pageviews']
		retval['PromedioSesión'] = data['avgSessionDuration']
		retval['PorcentajeRebote'] = data['bounceRate']
		retval['pagePath'] = data['pagePath']
		retval['Origen'] = getOrigen(data['Origen'])
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

class TraficoTotalPlayersLa100Map(Mapping):
	def sql(data : pd.DataFrame, end_date : str)-> pd.DataFrame:
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + getOrigen(data['Origen']) + 'TODOS'
		retval['Users'] = data['users']
		retval['Sessions'] = data['sessions']
		retval['Pageviews'] = data['pageviews']
		retval['PromedioSesión'] = data['avgSessionDuration']
		retval['PorcentajeRebote'] = data['bounceRate']
		retval['pagePath'] = 'TODOS'
		retval['Origen'] = getOrigen(data['Origen'])
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

class TraficoXHostnameMap(Mapping):
	def sql(data : pd.DataFrame, end_date : str)-> pd.DataFrame:
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + getOrigen(data['Origen']) + data['hostname']
		retval['Users'] = data['users']
		retval['Sessions'] = data['sessions']
		retval['Pageviews'] = data['pageviews']
		retval['PromedioSesión'] = data['avgSessionDuration']
		retval['PorcentajeRebote'] = data['bounceRate']
		retval['Hostname'] = data['hostname']
		retval['Origen'] = getOrigen(data['Origen'])
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

class TraficoXHostnameADCMap(Mapping):
	def sql(data : pd.DataFrame, end_date : str)-> pd.DataFrame:
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + getOrigen(data['Origen']) + 'TOTALADC'
		retval['Users'] = data['users']
		retval['Sessions'] = data['sessions']
		retval['Pageviews'] = data['pageviews']
		retval['PromedioSesión'] = data['avgSessionDuration']
		retval['PorcentajeRebote'] = data['bounceRate']
		retval['Hostname'] = 'TOTALADC'
		retval['Origen'] = getOrigen(data['Origen'])
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval

class TraficoXHostnameVerticalMap(Mapping):
	def sql(data : pd.DataFrame, end_date : str)-> pd.DataFrame:
		retval = pd.DataFrame()
		retval['UKEY'] = end_date + getOrigen(data['Origen']) + 'TOTALVERTICALES'
		retval['Users'] = data['users']
		retval['Sessions'] = data['sessions']
		retval['Pageviews'] = data['pageviews']
		retval['PromedioSesión'] = data['avgSessionDuration']
		retval['PorcentajeRebote'] = data['bounceRate']
		retval['Hostname'] = 'TOTALVERTICALES'
		retval['Origen'] = getOrigen(data['Origen'])
		retval['FechaFiltro'] = end_date
		retval['FechaCreacion'] = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S.000')
		retval['FechaModificacion'] = None
		return retval


def getOrigen(profile_name : pd.Series) -> str:
	if profile_name[0] == 'Red Cienradios':
		return 'RED CienRadios'
	elif profile_name[0] == 'La 100':
		return 'La100'
	elif profile_name[0] == 'Radio Mitre':
		return 'Mitre'
	elif profile_name[0] == 'Cienradios':
		return 'Cienradios'
	elif profile_name[0] == 'www.ciudad.com.ar':
		return 'Ciudad'
	elif profile_name[0] == 'El Doce tv':
		return 'ElDoce'
	elif profile_name[0] == 'Todos los sitios':
		return 'LosAndes'
	elif profile_name[0] == 'El Trece TV':
		return 'ElTrece'
	elif profile_name[0] == '01 - www.ole.com.ar':
		return 'Ole'
	elif profile_name[0] == 'TN Todo Noticias':
		return 'TN'
	elif profile_name[0] == '1 www.tycsports.com':
		return 'TyC'
	elif profile_name[0] == '01 - www.clarin.com':
		return 'Clarin'
	elif profile_name[0] == 'ViaPais':
		return 'ViaPais'
	else:
		return 'Error Mappeo'



