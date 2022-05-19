from datetime import datetime, timedelta
import requests
import json
from instagram.DailyInsight import DailyInsight
from instagram.Insight import Insight
from instagram.LifetimeInsight import LifetimeInsight

from instagram.Post import Post
from instagram.Story import Story

class Account:
	def __init__(self,facebook_id,facebook_name,token) -> None:
		self.facebook_id = facebook_id
		self.facebook_name = facebook_name
		self.token = token
		self.instagram_id = self.setInstagramId()
		self.instagram_name = self.setInstagramUsername()

	def setInstagramId(self) -> int:
		url = 'https://graph.facebook.com/'
		url += str(self.facebook_id)
		url += '?fields='
		url += 'instagram_business_account'
		url += '&access_token='
		url += self.token
		r = requests.get(url)
		response = json.loads(r.text)
		return response['instagram_business_account']['id']
	
	def setInstagramUsername(self) -> str:
		url = 'https://graph.facebook.com/'
		url += str(self.instagram_id)
		url += '?fields='
		url += 'username'
		url +='&access_token='
		url += self.token
		r = requests.get(url)
		message = json.loads(r.text)
		return message['username']

	def getPosts(self, n_days=10) -> list[Post]:
		# Configuro las fechas
		until = datetime.now().date() + timedelta(days=1)
		since = until - timedelta(days=n_days)
		# Formateo las fechas
		until = datetime.strftime(until,'%Y-%m-%d')
		since = datetime.strftime(since,'%Y-%m-%d')
		# Genero la URL
		url = 'https://graph.facebook.com/'
		url += self.instagram_id
		url += '/media'
		url += '?access_token='
		url += self.token
		url += '&fields='
		url += 'id,caption,like_count,comments_count,media_product_type,media_type,timestamp,username,insights.metric(engagement,impressions,reach,saved)'
		url += '&since=' + since
		url += '&until=' + until
		# Recupero los posteos
		posts = list[Post]()
		while url:
			r = requests.get(url)
			response = r.json()
			for p in response['data']:
				for i in p['insights']['data']:
					p[i['name']] = i['values'][0]['value']
				p.pop('insights')
				posts.append(Post(p))
			try:
				url = response['paging']['next']
			except KeyError:
				url = None
		return posts

	def getStories(self, n_days=10) -> list[Story]:
		# Configuro las fechas
		until = datetime.now().date() + timedelta(days=1)
		since = until - timedelta(days=n_days)
		# Formateo las fechas
		until = datetime.strftime(until,'%Y-%m-%d')
		since = datetime.strftime(since,'%Y-%m-%d')
		# Genero la URL
		url = 'https://graph.facebook.com/'
		url += self.instagram_id
		url += '/stories'
		url += '?access_token='
		url += self.token
		url += '&fields='
		url += 'id,caption,media_product_type,media_type,timestamp,username,insights.metric(exits,impressions,reach,replies,taps_forward,taps_back)'
		url += '&since=' + since
		url += '&until=' + until
		stories = list[Story]()
		while url:
			r = requests.get(url)
			response = r.json()
			for s in response['data']:
				for i in s['insights']['data']:
					s[i['name']] = i['values'][0]['value']
				s.pop('insights')
				stories.append(Story(s))
			try:
				url = response['paging']['next']
			except KeyError:
				url = None

		return stories

	def getDailyInsights(self, n_days=10) -> list[DailyInsight]:

		insights = list[Insight]()

		# Configuro las fechas
		until = datetime.now().date() + timedelta(days=1)
		since = until - timedelta(days=n_days)
		# Formateo las fechas
		until = datetime.strftime(until,'%Y-%m-%d')
		since = datetime.strftime(since,'%Y-%m-%d')

		# Daily Insights
		url = 'https://graph.facebook.com/'
		url += self.instagram_id
		url += '/insights'
		url += '?access_token='
		url += self.token
		url += '&metric='
		url += 'email_contacts,follower_count,get_directions_clicks,impressions,phone_call_clicks,profile_views,reach,text_message_clicks,website_clicks'
		url += '&period='
		url += 'day'
		url += '&since=' + since
		url += '&until=' + until
		r = requests.get(url)
		response = r.json()
		for i in response['data']:
			insights.append(DailyInsight(i))

		return insights

	def getLifetimeInsights(self, n_days=10) -> list[LifetimeInsight]:

		insights = list[Insight]()

		# Configuro las fechas
		until = datetime.now().date() + timedelta(days=1)
		since = until - timedelta(days=n_days)
		# Formateo las fechas
		until = datetime.strftime(until,'%Y-%m-%d')
		since = datetime.strftime(since,'%Y-%m-%d')

		# Lifetime Insights
		url = 'https://graph.facebook.com/'
		url += self.instagram_id
		url += '/insights'
		url += '?access_token='
		url += self.token
		url += '&metric='
		url += 'audience_city,audience_country,audience_gender_age,audience_locale'
		url += '&period='
		url += 'lifetime'
		r = requests.get(url)
		response = r.json()
		for i in response['data']:
			insights.append(LifetimeInsight(i))

		return insights