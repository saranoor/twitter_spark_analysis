import tweepy
from tweepy import Stream
#from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
import socket
import json

access_token = "1369013832446849024-625lwAVXaJevwzbKwSZyblcpPe1PHI"
access_secret = "ts7m7mCa9aTIPtf0PivyW7vc0i665ROtyUhkJQtIQvxV3"
consumer_key =  "8dzKja6GHdpGpLIrJBdGd7xmn"
consumer_secret = "BKHSMaLhVF98pfpFJuIWzGA2bPom0un5BSSkcurfyt5Ipp8nw2"

class TweetsListener(tweepy.Stream):
  # tweet object listens for the tweets
  def __init__(self, *args, csocket):
    super().__init__(*args)
    self.client_socket=csocket
  def on_status(self, status):
        print(status.id)
  # def on_data(self, data):
  #   try:
  #     msg = json.loads( data )
  #     print("new message")
  #     # if tweet is longer than 140 characters
  #     if "extended_tweet" in msg:
  #       # add at the end of each tweet "t_end"
  #       self.client_socket\
  #           .send(str(msg['extended_tweet']['full_text']+"t_end")\
  #           .encode('utf-8'))
  #       print(msg['extended_tweet']['full_text'])
  #     else:
  #       # add at the end of each tweet "t_end"
  #       self.client_socket\
  #           .send(str(msg['text']+"t_end")\
  #           .encode('utf-8'))
  #       print(msg['text'])
  #     return True
  #   except BaseException as e:
  #       print("Error on_data: %s" % str(e))
  #   return True
  # def on_error(self, status):
  #   print(status)
  #   return True

def sendData(c_socket, keyword):
  #print('start sending data from Twitter to socket')
  # authentication based on the credentials
  #auth = OAuthHandler(consumer_key, consumer_secret)
  #auth.set_access_token(access_token, access_secret)
  # start sending data from the Streaming API 
  access_token = "1369013832446849024-625lwAVXaJevwzbKwSZyblcpPe1PHI"
  access_secret = "ts7m7mCa9aTIPtf0PivyW7vc0i665ROtyUhkJQtIQvxV3"
  consumer_key =  "8dzKja6GHdpGpLIrJBdGd7xmn"
  consumer_secret = "BKHSMaLhVF98pfpFJuIWzGA2bPom0un5BSSkcurfyt5Ipp8nw2"  
  twitter_stream = TweetsListener(consumer_key,
  			          consumer_secret,
  			          access_token,
  			          access_secret        
  )
  twitter_stream.sample()
  twitter_stream.filter(track = keyword, languages=["en"])

if __name__ == "__main__":
    # server (local machine) creates listening socket
    s = socket.socket()
    host = "0.0.0.0"    
    port = 5555
    s.bind((host, port))
    print('socket is ready')
    # server (local machine) listens for connections
    s.listen(4)
    print('socket is listening')
    # return the socket and the address on the other side of the connection (client side)
    c_socket, addr = s.accept()
    print("Received request from: " + str(addr))
    # select here the keyword for the tweet data
    sendData(c_socket, keyword = ['piano'])
