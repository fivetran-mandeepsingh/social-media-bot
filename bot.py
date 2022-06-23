import re
import json
import os
import string
import tweepy
from tweepy import OAuthHandler
from textblob import TextBlob
import requests
import enum

class TwitterClient(object):
	'''
	Generic Twitter Class for sentiment analysis.
	'''
	def __init__(self):
		'''
		Class constructor or initialization method.
		'''
		filepath = os.path.expanduser('~/creds.txt')
		
		with open(filepath) as f:
			# keys and tokens from the Twitter Dev Console
			data = json.load(f)
			consumer_key = data['consumer_key']
			consumer_secret = data['consumer_secret']
			access_token = data['access_token']
			access_token_secret = data['access_token_secret']
			url_generator_access_token = data['url_generator_access_token']

		# attempt authentication
		try:
			# create OAuthHandler object
			self.auth = OAuthHandler(consumer_key, consumer_secret)
			# set access token and secret
			self.auth.set_access_token(access_token, access_token_secret)
			# create tweepy API object to fetch tweets
			self.api = tweepy.API(self.auth)
			# create object for url generation
			self.url_generator = ShortUrlGenerator(url_generator_access_token)
			self.tweet_reply_generator = TweetReplyGenerator(self.url_generator)
		except:
			print("Error: Authentication Failed")

	def clean_tweet(self, tweet):
		'''
		Utility function to clean tweet text by removing links, special characters
		using simple regex statements.
		'''
		return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

	def get_tweet_sentiment(self, tweet):
		'''
		Utility function to classify sentiment of passed tweet
		using textblob's sentiment method
		'''
		# create TextBlob object of passed tweet text
		analysis = TextBlob(self.clean_tweet(tweet))
		# set sentiment
		if analysis.sentiment.polarity > 0:
			return 'positive'
		elif analysis.sentiment.polarity == 0:
			return 'neutral'
		else:
			return 'negative'

	def get_tweets(self, query, count = 10):
		'''
		Main function to fetch tweets and parse them.
		'''
		# empty list to store parsed tweets
		tweets = []

		try:
			# call twitter api to fetch tweets
			fetched_tweets = self.api.search_tweets(q = query, count = count)

			# parsing tweets one by one
			for tweet in fetched_tweets:
				# empty dictionary to store required params of a tweet
				parsed_tweet = {}

				# saving text of tweet
				parsed_tweet['text'] = tweet.text
				# saving sentiment of tweet
				parsed_tweet['sentiment'] = self.get_tweet_sentiment(tweet.text)

				# appending parsed tweet to tweets list
				if tweet.retweet_count > 0:
					# if tweet has retweets, ensure that it is appended only once
					if parsed_tweet not in tweets:
						tweets.append(parsed_tweet)
				else:
					tweets.append(parsed_tweet)

			# return parsed tweets
			return tweets

		except tweepy.TweepError as e:
			# print error (if any)
			print("Error : " + str(e))


	def get_tweet_reply(self):
		return self.tweet_reply_generator.getReply("looking for data pipelinf",Sentiment.NEGATIVE)

def main():
	# creating object of TwitterClient Class
	api = TwitterClient()
	# calling function to get tweets
	tweets = api.get_tweets(query = 'Narendra Modi', count = 50)

	# picking positive tweets from tweets
	ptweets = [tweet for tweet in tweets if tweet['sentiment'] == 'positive']
	# percentage of positive tweets
	print("Positive tweets percentage: {} %".format(100*len(ptweets)/len(tweets)))
	# picking negative tweets from tweets
	ntweets = [tweet for tweet in tweets if tweet['sentiment'] == 'negative']
	# percentage of negative tweets
	print("Negative tweets percentage: {} %".format(100*len(ntweets)/len(tweets)))
	# picking neutral tweets from tweets
	neutweets = [tweet for tweet in tweets if tweet['sentiment'] == 'neutral']
	# percentage of neutral tweets
	print("Neutral tweets percentage: {} % \
		".format(100*len(neutweets)/len(tweets)))

	# printing all positive tweets
	print("\n\nPositive tweets:")
	for index, tweet in enumerate(ptweets):
		print(str(index+1) + ". " + tweet['text'])

	# printing all negative tweets
	print("\n\nNegative tweets:")
	for index, tweet in enumerate(ntweets):
		print(str(index+1) + ". " + tweet['text'])

	# printing all neutral tweets
	print("\n\nNeutral tweets:")
	for index, tweet in enumerate(neutweets):
		print(str(index+1) + ". " + tweet['text'])


	print(api.get_tweet_reply())

class Sentiment(enum.Enum):
	POSITIVE = 1
	NEGATIVE = -1
	NEUTRAL = 0

class TweetReplyGenerator():
	'''
	Class to generate reply for the tweets based on the sentiment of the tweet
	'''
	CONNECTORS = ["salesforce","netsuite","zuora","outreach"]
	CUSTOMER_SUPPORT_LINK = "https://support.fivetran.com/hc/en-us"
	CONNECTORS_COMING_SOON = "https://www.fivetran.com/connectors?status=soon"
	NEW_CONNECTOR_REQUEST = "https://support.fivetran.com/hc/en-us/community/topics/360001909373-Feature-Requests"
	FIVETRAN_LINK = "https://www.fivetran.com/"


	def __init__(self,short_url_generator):
		self.short_url_generator = short_url_generator


	def getReply(self,tweet,sentiment):
		if(str(tweet).lower().find("fivetran")!=-1):
			if Sentiment.NEGATIVE == sentiment or Sentiment.NEUTRAL == sentiment:
				return self.getReplyForNegativeTweetWithFivetran(str(tweet))
			else:
				return self.getReplyForPositiveTweetWithFivetran()

		else:
			return self.getReplyForPossibbleOpportunity()

 


	def getReplyForNegativeTweetWithFivetran(self,tweet):
		if(tweet.lower().find("failing")!=-1):
			return self.getReplyForConnectorFailing(tweet)
		elif(tweet.lower().find("pricing")!=-1 or tweet.lower().find("expensive")!=-1 or tweet.lower().find("costly")!=-1 or tweet.lower().find("costly")!=-1):
			return self.getReplyForPricingIssue()
		return self.getRelyForConnecToCustomerSupport()
		

	def getReplyForPositiveTweetWithFivetran(self):

		reply =  "Glad to hear you enjoyed our product, check out new connectors coming soon "
		reply += self.short_url_generator.get_short_url(TweetReplyGenerator.CONNECTORS_COMING_SOON)
		reply += ", request for connectors here "+ self.short_url_generator.get_short_url(TweetReplyGenerator.NEW_CONNECTOR_REQUEST)

		return reply

	def getReplyForPricingIssue(self):
		txt = "Sorry for the inconvenience you faced with our with our pricing model, "
		return txt+ self.getRelyForConnecToCustomerSupport()


	def getReplyForConnectorFailing(self,tweet):

		txt = "Sorry for the inconvenience you faced with our{connector:s} connector, "
		connector = ""

		for c in TweetReplyGenerator.CONNECTORS:
			if(str(tweet).find(c)!=-1):
				connector=" "+c
				break

		return txt.format(connector=connector)+ self.getRelyForConnecToCustomerSupport()




	def getRelyForConnecToCustomerSupport(self):
		reply = "Please connect with out customer support for the resolution "
		reply = reply + self.short_url_generator.get_short_url(self.CUSTOMER_SUPPORT_LINK)
		return reply


	def getReplyForPossibbleOpportunity(self):

		reply = "We offer the industry's best selction of fully managed connectors, "
		reply =  reply + "Our pipelines automatically and continuously update, freeing you up to focus on game-changing insighta instead of ETL. Check out our product "
		reply += self.short_url_generator.get_short_url( TweetReplyGenerator.FIVETRAN_LINK)
		return reply


class ShortUrlGenerator(object):
	'''
	Class to create and return short URLs for long URLs
	'''
	def __init__(self, access_token):
		self.access_token = access_token
	
	def get_short_url(long_url):
		headers = {
			'Authorization': 'Bearer {self.access_token}',
			'Content-Type': 'application/json',
		}
		data = '{ "long_url": "{long_url}"}'

		response = requests.post('https://api-ssl.bitly.com/v4/shorten', headers=headers, data=data)
		return response

class ShortUrlGenerator(object):
	'''
	Class to create and return short URLs for long URLs
	'''
	def __init__(self, access_token):
		self.access_token = access_token
	
	def get_short_url(self, long_url):
		headers = {
			'Authorization': 'Bearer %(access_token)s' % { "access_token": self.access_token },
			'Content-Type': 'application/json',
		}
		data = '{ "long_url": "%(long_url)s"}' % { "long_url":long_url }

		# response = requests.post('https://api-ssl.bitly.com/v4/shorten', headers=headers, data=data)
		# return response.json()['link']
		return "https://bit.ly/3HJ6HB6"

if __name__ == "__main__":
	# calling main function
	main()
