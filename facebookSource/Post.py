import requests
import json
from mappings.facebookSource.Post import PostMapping

class Post:
	def __init__(self, name,*,id = None, token = None, data = None) -> None:

		''' Un posteo de Facebook a partir de:
			- Un ID sacado de un archivo (id, token)
			- Un json devuelto por la GRAPH API (data)
			- Argumento obligatorio (name): nombre de la cuenta
		'''

		self.name = name
		if data is None:
			self.id = id
			self.token = token
			self.data = self.getData()
		else:
			self.data = data
			self.data['pagename'] = self.name

	def asRawDict(self):
		return PostMapping.raw(self.data)

	def asCleanDict(self):
		return PostMapping.clean(self.data)
	
	def asSQLDict(self):
		return PostMapping.sql(self.data)

	def getData(self):

		'''A partir de un ID recupera data historica del posteo'''

		url = 'https://graph.facebook.com/'
		url += self.id
		url += '?access_token='
		url += self.token
		url += '&fields=id, message, created_time, updated_time, attachments,'
		url += 'insights.metric(post_activity_by_action_type,post_impressions,post_engaged_users)'
		url += '&since=0'
		r = requests.get(url)
		response = json.loads(r.text)
		response['pagename'] = self.name
		return response

		