import re
import json
import os
import string
import tweepy
from tweepy import OAuthHandler
from textblob import TextBlob
import requests
import enum
from transformers import pipeline

class TwitterClient(object):
	'''
	Generic Twitter Class for sentiment analysis.
	'''
	def __init__(self, consumer_key, consumer_secret, access_token, access_token_secret):
		'''
		Class constructor or initialization method.
		'''
		# attempt authentication
		try:
			# create OAuthHandler object
			self.auth = OAuthHandler(consumer_key, consumer_secret)
			# set access token and secret
			self.auth.set_access_token(access_token, access_token_secret)
			# create tweepy API object to fetch tweets
			self.api = tweepy.API(self.auth)
		except Exception as e:
			print("Exception occured while Authenticating Twitter api. Error: " + str(e))
		
		self.model = pipeline(model="finiteautomata/bertweet-base-sentiment-analysis")

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
		analysis = self.model(self.clean_tweet(tweet))
		# set sentiment
		if analysis[0]['label'] == 'POS':
			return 'positive'
		elif analysis[0]['label'] == 'NEG':
			return 'negative'
		else:
			return 'neutral'
	
	def limit_handled(self, cursor):
		while True:
			try:
				yield cursor.next()
			except StopIteration:
				break

	def get_tweets(self, query, count = 10):
		'''
		Main function to fetch tweets and parse them.
		'''
		# empty list to store parsed tweets
		tweets = []
		# call twitter api to fetch tweets
		# fetched_tweets = self.api.search_tweets(q = query, count = count)
		# fetched_tweets = self.limit_handled(tweepy.Cursor(self.api.search_tweets,
        #                q=query,
        #                tweet_mode='extended',
        #                lang='en',
        #                result_type='recent').items(count))

		fetched_tweets = [
			"I love how easy to setup fivetran's connectors are",
			"Can anyone help me find how to send my salesforce data into a data warehouse for further analysis",
			"I hate Fivetran's pricing structure",
			"Is there a cheap way to send my data to data warehouse for analysis",
            "Fivetran's postgres connector keeps failing i don't know why",
            "How to do data analysis on my google sheet data? Any idea?"
        ]

		# parsing tweets one by one
		for tweet in fetched_tweets:
			# empty dictionary to store required params of a tweet
			parsed_tweet = {}

			# saving text of tweet
			parsed_tweet['text'] = tweet
			# saving sentiment of tweet
			parsed_tweet['sentiment'] = self.get_tweet_sentiment(tweet)
			parsed_tweet['id'] = 1

			# appending parsed tweet to tweets list
			if False:
				# if tweet has retweets, ensure that it is appended only once
				if parsed_tweet not in tweets:
					tweets.append(parsed_tweet)
			else:
				tweets.append(parsed_tweet)

		# return parsed tweets
		return tweets

	def reply_to_tweet(self, tweet_id, reply_text):
		try:
			print("Mock response: " + reply_text)
			# response = self.api.update_status(status = reply_text, in_reply_to_status_id=tweet_id, auto_populate_reply_metadata=True)
		except Exception as e:
			print("Exception while replying to tweet. Error: " + str(e))
			raise e

def main():
	# read creds
	filepath = os.path.expanduser('~/creds.txt')
	
	with open(filepath) as f:
		# keys and tokens from the Twitter Dev Console
		data = json.load(f)
		consumer_key = data['consumer_key']
		consumer_secret = data['consumer_secret']
		access_token = data['access_token']
		access_token_secret = data['access_token_secret']
		url_generator_access_token = data['url_generator_access_token']

	# creating object of TwitterClient Class
	api = TwitterClient(consumer_key, consumer_secret, access_token, access_token_secret)
	# calling function to get tweets
	# tweets = api.get_tweets(query = 'Fivetran OR (Extract Transform Load) OR (Extract  Load Transform) OR (data pipeline)', count = 10)
	tweets = api.get_tweets(query = '#LoadDataIntoDataWarehouse', count = 10)

	try:
		# create object for url generation
		tweet_reply_generator = TweetReplyGenerator(ShortUrlGenerator(url_generator_access_token))
	except Exception as e:
		print("Exception occured while Authenticating Bitly. Error: " + str(e))
	
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
		reply = tweet_reply_generator.getReply(tweet['text'], Sentiment.POSITIVE)
		api.reply_to_tweet(tweet['id'], reply)

	# printing all negative tweets
	print("\n\nNegative tweets:")
	for index, tweet in enumerate(ntweets):
		print(str(index+1) + ". " + tweet['text'])
		reply = tweet_reply_generator.getReply(tweet['text'], Sentiment.NEGATIVE)
		api.reply_to_tweet(tweet['id'], reply)

	# printing all neutral tweets
	print("\n\nNeutral tweets:")
	for index, tweet in enumerate(neutweets):
		print(str(index+1) + ". " + tweet['text'])
		reply = tweet_reply_generator.getReply(tweet['text'], Sentiment.NEUTRAL)
		api.reply_to_tweet(tweet['id'], reply)

class Sentiment(enum.Enum):
	POSITIVE = 1
	NEGATIVE = -1
	NEUTRAL = 0

class TweetReplyGenerator():
	'''
	Class to generate reply for the tweets based on the sentiment of the tweet
	'''
	CONNECTORS = ["salesforce","google sheet","postgres"]
	CONNECTORS_DOCUMENTATTION={}
	CONNECTORS_DOCUMENTATTION["salesforce"]="https://fivetran.com/docs/applications/salesforce"
	CONNECTORS_DOCUMENTATTION["google sheet"]="https://fivetran.com/docs/files/google-sheets"
	CONNECTORS_DOCUMENTATTION["postgres"]="https://fivetran.com/docs/databases/postgresql"
	CUSTOMER_SUPPORT_LINK = "https://support.fivetran.com/hc/en-us"
	CONNECTORS_COMING_SOON = "https://www.fivetran.com/connectors?status=soon"
	NEW_CONNECTOR_REQUEST = "https://support.fivetran.com/hc/en-us/community/topics/360001909373-Feature-Requests"
	FIVETRAN_LINK = "https://www.fivetran.com/"
	PRICING = "https://www.fivetran.com/pricing"

	def __init__(self,short_url_generator):
		self.short_url_generator = short_url_generator


	def getReply(self,tweet,sentiment):
		if(str(tweet).lower().find("fivetran")!=-1):
			if Sentiment.NEGATIVE == sentiment or Sentiment.NEUTRAL == sentiment:
				return self.getReplyForNegativeTweetWithFivetran(str(tweet))
			else:
				return self.getReplyForPositiveTweetWithFivetran()
		else:
			return self.getReplyForPossibbleOpportunity(str(tweet))

	def getReplyForNegativeTweetWithFivetran(self,tweet):
		if(tweet.lower().find("failing")!=-1):
			return self.getReplyForConnectorFailing(tweet)
		elif(tweet.lower().find("pricing")!=-1 or tweet.lower().find("expensive")!=-1 or tweet.lower().find("costly")!=-1 or tweet.lower().find("cost")!=-1):
			return self.getReplyForPricingIssue()
		return self.getRelyForConnecToCustomerSupport()
		
	def getReplyForPositiveTweetWithFivetran(self):
		reply =  "Glad to know that you enjoyed using Fivetran. You can check out the new connectors that we are building here: "
		reply += self.short_url_generator.get_short_url(TweetReplyGenerator.CONNECTORS_COMING_SOON)
		reply += ". You can also submit request for a new connector here: " + self.short_url_generator.get_short_url(TweetReplyGenerator.NEW_CONNECTOR_REQUEST)
		return reply

	def getReplyForPricingIssue(self):
		txt = "Sorry for the inconvenience you faced with with our pricing model. "
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
		reply = "Please connect with our customer support at support@fivetran.com for quick resolution."
		return reply

	def getReplyForPossibbleOpportunity(self,tweet):
		reply = "Fivetran provides a fast, secure and easy to setup data pipeline which helps you centralize your data in minutes. "
		for c in TweetReplyGenerator.CONNECTORS:
			if(tweet.lower().find(c)!=-1):
				reply = reply + "You can checkout our "+ c+" connector "+self.short_url_generator.get_short_url(TweetReplyGenerator.CONNECTORS_DOCUMENTATTION[c])
				return reply;
		if(tweet.lower().find("pricing")!=-1 or tweet.lower().find("expensive")!=-1 or tweet.lower().find("costly")!=-1 or tweet.lower().find("cost")!=-1):
			reply = reply + " Checkout our pricing "+self.short_url_generator.get_short_url(TweetReplyGenerator.PRICING)
			return reply
		reply =  reply + "Our pipelines automatically and continuously update, freeing you up to focus on game-changing insighta instead of ETL. Check out our product "
		reply += self.short_url_generator.get_short_url(TweetReplyGenerator.FIVETRAN_LINK)
		return reply


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
		# print("Short url generated: " + str(response.json()['link']))
		# return response.json()['link']
		return "https://bit.ly/3HJ6HB6"

if __name__ == "__main__":
	# calling main function
	main()
	# api = TwitterClient()
	# tweets = api.reply_to_tweet(1539954418820362241, "Try out Fivetran with a 14 days free Trail account!")
