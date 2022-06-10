from twitter.Mention import Mention
from twitter.Tweet import Tweet
import tweepy

from twitter.User import User

class Account:
	def __init__(self, data) -> None:
		self.name = data['name']
		self.APIKey = data['APIKey']
		self.APIKeySecret = data['APIKeySecret']
		self.BearerToken = data['BearerToken']
		self.AccessToken = data['AccessToken']
		self.AccessTokenSecret = data['AccessTokenSecret']
		self.user = data['user_name']
		self.limit = 1000
		self.apiv1 = self.createAPIV1()
		self.users = self.loadUsers()
		# self.apiv2 = self.createAPIV2()

	def createAPIV1(self):
		auth = tweepy.OAuthHandler(self.APIKey, self.APIKeySecret)
		auth.set_access_token(self.AccessToken, self.AccessTokenSecret)
		api = tweepy.API(auth, wait_on_rate_limit=True)
		return api

	# def createAPIV2(self):
	# 	# Creating the authentication object
	# 	auth = tweepy.OAuthHandler(self.APIKey, self.APIKeySecret)
	# 	# Setting your access token and secret
	# 	auth.set_access_token(self.AccessToken, self.AccessTokenSecret)
	# 	# Creating the API object while passing in auth information
	# 	api = tweepy.API(auth, wait_on_rate_limit=True)
	# 	return api

	def getTweets(self) -> list[Tweet]:
		retval = list[Tweet]()
		for tweet in tweepy.Cursor(self.apiv1.user_timeline, screen_name=self.user, tweet_mode='extended').items(self.limit):
			retval.append(Tweet(tweet._json))
		return retval

	def getMentions(self) -> list[Mention]:
		retval = list[Mention]()
		for tweet in tweepy.Cursor(self.apiv1.mentions_timeline, trim_user=False, tweet_mode='extended').items(self.limit):
			retval.append(Mention(tweet._json))
		return retval

	def getUsers(self) -> list[User]:
		retval = list[User]()
		for user in self.users:
			retval.append(User(self.apiv1.get_user(screen_name=user)._json))
		return retval

	def loadUsers(self):
		return ['cadena3com','ciudad_magazine','clarincom','continental590','diariouno','eldoce','eltreceoficial','lavozcomar'
				,'infobae','la100fm','lanacion','radiolared','losandesdiario','mdzol','metro951','diarioole','primiciasyacom'
				,'radiomitre','telefe','todonoticias','tycsports','vorterix']