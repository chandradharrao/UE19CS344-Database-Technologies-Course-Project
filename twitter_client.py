from multiprocessing.connection import Listener
import os
from time import time
import requests
from dotenv import load_dotenv
import requests_oauthlib
import tweepy
from tweepy import Stream as twitterStream

load_dotenv()

#fucntions
def isEng(hashtag):
    #check if string is ascii encoded or not
    try:
        hashtag.encode(encoding='utf-8').decode('ascii')
    except Exception as e:
        return False
    else:
        return True

#keys
API_KEY = os.environ.get("API_KEY")
BEARER_TOKEN = os.environ.get("BEARER_TOKEN")
API_SECRET = os.environ.get("API_SECRET")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")
ACCESS_TOKEN_SECRET = os.environ.get("ACCESS_TOKEN_SECRET")

#globals
WOEID=23424848

#auth
print(API_KEY,API_SECRET,ACCESS_TOKEN,ACCESS_TOKEN_SECRET)
auth = tweepy.OAuthHandler(API_KEY,API_SECRET)
auth.set_access_token(ACCESS_TOKEN,ACCESS_TOKEN_SECRET)

#api instance
api = tweepy.API(auth)

#trending hashtags
# top5 = ['' for i in range(5)]
# trend_res = api.get_place_trends(WOEID)[0]['trends']

# i = 0
# for trend in trend_res:
#     if trend['name'][0]=='#' and isEng(trend['name']):
#         top5[i] = trend['name'][1::]
#         i+=1
#         if i>=5:
#             break
# print(top5)

top5 = ['RamVsYash', 'DelhiRiots', 'sundayvibes', 'jahagirpuri', '#HindusUnderAttackInIndia']

class StreamHandler(twitterStream):
    #on recieving a tweet
    def on_status(self, status):
        print(status.user.screen_name,"tweeted",status.text)

def streamTweets():
    myStreamHandler = StreamHandler(API_KEY,API_SECRET,ACCESS_TOKEN,ACCESS_TOKEN_SECRET)
    myStreamHandler.filter(track=top5)

#realtime tweet stream
streamTweets()