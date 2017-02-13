import tweepy #
import sqlite3
from time import sleep
from datetime import datetime
import sys
import json
import csv
from random import shuffle
import gzip

data_dir = "raw_data/"
profiles_dir = "gzipped_nssec_profiles/"
dlu_file = "gzipped_nssec_downloaded_users"
m_id = 806287101348155392
api = None

def get_all_tweets(user_id, limit_calls=None, max_id=None):
    global api
    #Twitter only allows access to a users most recent 3240 tweets with this method
    print "Downloading user:", user_id
    #initialize a list to hold all the tweepy Tweets
    alltweets = []

    #make initial request for most recent tweets (200 is the maximum allowed count)
    per_call = 200
    if max_id:
        new_tweets = api.user_timeline(id=user_id, count=per_call, max_id=max_id)
    else:
        new_tweets = api.user_timeline(id=user_id, count=per_call)
    calls = 1
    #save most recent tweets
    alltweets.extend(new_tweets)

    #save the id of the oldest tweet less one
    if alltweets:
        oldest = alltweets[-1].id - 1

        #keep grabbing tweets until there are no tweets left to grab
        while len(new_tweets) > 0:
            # print "getting tweets before %s" % (oldest)
            if limit_calls:
                if calls >= limit_calls:
                    break

            #all subsiquent requests use the max_id param to prevent duplicates
            new_tweets = api.user_timeline(id=user_id, count=per_call, max_id=oldest)
            calls += 1
            #save most recent tweets
            alltweets.extend(new_tweets)

            #update the id of the oldest tweet less one
            oldest = alltweets[-1].id - 1
        print str(len(alltweets)), "tweets downloaded."
    else:
        print "No tweets downloaded."
    return alltweets

def main():
    global api
    consumer_key, consumer_secret, access_key, access_secret = [key.strip() for key in open("keys")]
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)


    print "Starting."
    with open(data_dir+dlu_file,"a+") as dled_users_handle:
        downloaded_users = set([int(line.strip().split(",")[0]) for line in dled_users_handle])
    with open(data_dir+"new_users.txt") as users_to_dl_handle:
        users_to_download = [int(line.strip()) for line in users_to_dl_handle if int(line.strip()) not in downloaded_users]
        shuffle(users_to_download)
    for uid in users_to_download:
        try:
            alltweets = get_all_tweets(uid, max_id=m_id)
            if alltweets:
                with gzip.open(data_dir+profiles_dir+str(uid)+".gz","w+") as f:
                    for tweet in alltweets:
                        decoded = tweet._json
                        f.write(json.dumps(decoded)+"\n")
                        downloaded_users.add(uid)
                    with open(data_dir+dlu_file,"a+",0) as dled_users_handle:
                        dled_users_handle.write(str(uid)+"\n")
            else:
                with open(data_dir+dlu_file,"a+",0) as dled_users_handle:
                    dled_users_handle.write(str(uid)+",no_tweets\n")
                downloaded_users.add(uid)
        except KeyboardInterrupt:
            raise KeyboardInterrupt()
        except tweepy.TweepError, e:
            with open(data_dir+dlu_file,"a+") as dled_users_handle:
                dled_users_handle.write(str(uid)+",exception:"+str(e)+"\n")
            downloaded_users.add(uid)
            print "Tweepy error:",str(e)
        except Exception, e:
            raise




if __name__ == '__main__':
    while True:
        try:
            main()
        except KeyboardInterrupt:
            print "System exiting due to keyboard interrupt."
            sys.exit()
        except Exception, e:
            print "Error in main:", str(e)
            sleep(30)