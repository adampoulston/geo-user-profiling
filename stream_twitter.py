import tweepy
import json
from time import sleep
from datetime import datetime
import os

# load keys from file handily named keys
consumer_key, consumer_secret, access_token, access_token_secret = [key.strip() for key in open("keys")]

def get_filename(dt):
    fn = dt.strftime("%Y-%m-%d_%H")
    return fn

# stream listener implementation
class StdOutListener(tweepy.StreamListener):
    def __init__(self):
        #in case of error sleep for this many seconds
        self.sleep_for = 30

        #file names/handles for storing tweets
        self.fn = None
        self.fh = None

        self.data_dir = "raw_data/"
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)

        self.tweet_data_dir = self.data_dir+"tweet_stream/"
        if not os.path.exists(self.tweet_data_dir):
            os.makedirs(self.tweet_data_dir)

        self.date_format = "%a %b %d %H:%M:%S +0000 %Y"

        #file handle for recording users to download
        self.td_fh = open(self.data_dir+"users_to_download", "a+", 0)
        self.seen_users = set([int(line.strip()) for line in self.td_fh])



    def on_data(self, data):
        try:
            '''Assess and store streamed tweet'''
            #load tweet into dictionary
            decoded = json.loads(data)
            
            #check message is not empty/error
            if 'user' in decoded and 'text' in decoded:
                #remove newlines but replace with character for thouroughness
                out_data = data.strip().replace("\r", "").replace("\n", "<newline>")+"\n"

                #check whether or not we want to roll a new file
                fn = get_filename(datetime.strptime(decoded['created_at'], self.date_format))
                if fn != self.fn:
                    self.fn = fn
                    try: self.fh.close()
                    except: pass
                    self.fh = open(self.tweet_data_dir+self.fn,"a+", 0)

                #store tweet and user if geolocated and not seen yet
                self.fh.write(out_data)
                if decoded["user"]["id"] not in self.seen_users:
                    if decoded['coordinates']:
                        self.seen_users.add(decoded["user"]["id"])
                        self.td_fh.write(str(decoded["user"]["id"])+"\n")
        except Exception, e:
            print "Programming error:",str(e)

        return True

    def on_error(self, status):
        print "Twitter error:",status

def main():
    try:
        #OAuth setut
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        # init listener
        l = StdOutListener()
        stream = tweepy.Stream(auth, l)

        #Listen on uk bounding box (change/remove if needed)
        GEO_UK = [-5.2281708717,49.9995286556,2.0106048584,59.2419967352]
        print "Viewing new tweets within",GEO_UK

        stream.filter(locations=GEO_UK)

    except KeyboardInterrupt:
        print "Exiting due to keyboard interrupt."

    except Exception, e:
        #TODO: Handle this in a way that isn't terrible
        #if an error occurs sleep until it go's away
        # print "Error:",str(e)
        # sleep(30)
        # main()
        raise
    finally:
        try: self.fh.close()
        except: pass

        try: self.td_fh.close()
        except: pass


if __name__ == '__main__':
    main()
    