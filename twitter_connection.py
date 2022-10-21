import tweepy
from tweepy import Stream
#from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
import socket
import json
access_token = "1369013832446849024-625lwAVXaJevwzbKwSZyblcpPe1PHI"
access_secret = "ts7m7mCa9aTIPtf0PivyW7vc0i665ROtyUhkJQtIQvxV3"
consumer_key =  "8dzKja6GHdpGpLIrJBdGd7xmn"
consumer_secret =  "BKHSMaLhVF98pfpFJuIWzGA2bPom0un5BSSkcurfyt5Ipp8nw2"

class TweetsListener(tweepy.Stream):
  def __init__(self, cons_key, cons_secret, token, token_sec, csocket):
      super().__init__(cons_key, cons_secret, token, token_sec)
      self.client_socket = csocket

  def on_status(self, status):
        print(status.id)

  def on_data(self, data):
    try:
      msg = json.loads( data )
      #print("new message", msg)
      # if tweet is longer than 140 characters
      if "extended_tweet" in msg:
        # add at the end of each tweet "t_end"
        string_to_send= msg['extended_tweet']['full_text']
        string_to_send=string_to_send.replace("\n",' ')
        self.client_socket\
            .send(str(str(msg['id'])+","+ str(msg['id_str'])+string_to_send+" t_end \n").encode('utf-8'))
        print(str(str(msg['id'])+","+str(msg['id_str'])+msg['extended_tweet']['full_text']+"t_end").encode('utf-8'))
        print(str(str(msg['id'])+","+str(msg['id_str'])+msg['extended_tweet']['full_text']+"t_end"))

      # else:
      #   # add at the end of each tweet "t_end"
      #   self.client_socket\
      #       .send(str(str(msg['id'])+","+str(msg['id_str'])+msg['text']+"t_end").encode('utf-8'))
      #   print(str(str(msg['id'])+","+str(msg['id_str'])+msg['text']+"t_end").encode('utf-8'))
      return True
    except BaseException as e:
        print("Error on_data: %s" % str(e))
    return True

  def on_error(self, status):
    return True

def sendData(c_socket, keyword):
  print('start sending data from Twitter to socket')
  access_token = "1369013832446849024-625lwAVXaJevwzbKwSZyblcpPe1PHI"
  access_secret = "ts7m7mCa9aTIPtf0PivyW7vc0i665ROtyUhkJQtIQvxV3"
  consumer_key =  "8dzKja6GHdpGpLIrJBdGd7xmn"
  consumer_secret = "BKHSMaLhVF98pfpFJuIWzGA2bPom0un5BSSkcurfyt5Ipp8nw2"
  twitter_stream = TweetsListener(consumer_key, consumer_secret,
                                 access_token, access_secret, c_socket)
  twitter_stream.filter(track=["kind"])

if __name__ == "__main__":
    # server (local machine) creates listening socket
    s = socket.socket()
    host = "0.0.0.0"
    port = 9999
    s.bind((host, port))
    print('socket is ready')
    # server (local machine) listens for connections
    s.listen(4)
    print('socket is listening')
    # return the socket and the address on the other side of the connection (client side)
    c_socket, addr = s.accept()
    print("Received request from: " + str(addr))
    # select here the keyword for the tweet data
    sendData(c_socket, keyword = 'piano')