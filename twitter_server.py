'''
This is a streaming server that will stream the tweets to clients that are connected to it.
'''

from curses import raw
import json
from tweepy import StreamListener
import os
import socket
from time import time
from tkinter import E
import requests
from dotenv import load_dotenv
import requests_oauthlib
import tweepy
from tweepy import Stream
from tweepy.streaming import Stream

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

class StreamHandler(StreamListener):
    #connect to socket to which we want to send data
    def __init__(self,sock):
        self.sendto_socket = sock

    def on_data(self, raw_data):
        try:
            tweet = json.loads(raw_data)
            screen_name = tweet['user']['screen_name']
            text = tweet['text']
            for hashtag in tweet['entities']['hashtags']:
                print("---------START------------")
                print(screen_name,hashtag['text'],text)
                print("----------END------------")

                #send data to the socker
                data = f"{hashtag},{screen_name},{text}\n"
                res = self.sendto_socket.send(data.encode())
                print("bytes sent:",res)
                return True
        except Exception as e:
            print("[ERROR]:",e)
        return True

    def if_error(self,status):
        print(status)
        return True

def streamTweets(sock):
    twitter_stream = Stream(auth,StreamHandler(sock))
    twitter_stream.filter(track=top5)

if __name__ == "__main__":
    #create open socket for a client to connect
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    host = "localhost"
    port = 9090
    sock.bind((host,port))
    
    print(f"Listening on port {port}....")
    #wait for a client to connect
    sock.listen(5)
    client_sock,client_addr = sock.accept()

    print(f"Recieved connection request from {str(client_addr)}")
    #stream the data to the client
    streamTweets(client_sock)
