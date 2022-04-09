import requests
import json
from mappings.facebookSource.Post import PostMongoRaw, PostMongoClean, PostSQL


class Post:
	def __init__(self,data :dict, token) -> None:

		'''Un posteo con sus estadisticas'''

		self.data = data
		self.id = data['id']
		self.token = token
		self.loadReactions()

	def asRawDict(self):
		return PostMongoRaw.clean(self.data)

	def asCleanDict(self):
		return PostMongoClean.clean(self.data)
	
	def asSQLDict(self):
		return PostSQL.clean(self.data)

	def loadReactions(self):

		'''Carga likes, comentarios y shares del posteo'''

		url = 'https://graph.facebook.com/'
		url += self.id
		url += '/insights'
		url += '?access_token='
		url += self.token
		url += '&metric=post_activity_by_action_type'
		r = requests.get(url)
		response = json.loads(r.text)
		try:
			self.data['likes'] = response['data'][0]['values'][0]['value']['like']
		except:
			self.data['likes'] = 0
		try:
			self.data['comments'] = response['data'][0]['values'][0]['value']['comment']
		except:
			self.data['comments'] = 0
		try:
			self.data['shares'] = response['data'][0]['values'][0]['value']['share']
		except:
			self.data['shares'] = 0
		