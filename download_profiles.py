import tweepy #
import sqlite3
from time import sleep
from datetime import datetime
import sys
import json

data_dir = "raw_data/"
profiles_dir = "profiles/"

api = None

def get_all_tweets(user_id):
    global api
    #Twitter only allows access to a users most recent 3240 tweets with this method
    print "Downloading user:", user_id
    #initialize a list to hold all the tweepy Tweets
    alltweets = []

    #make initial request for most recent tweets (200 is the maximum allowed count)
    new_tweets = api.user_timeline(id=user_id, count=200)

    #save most recent tweets
    alltweets.extend(new_tweets)

    #save the id of the oldest tweet less one
    oldest = alltweets[-1].id - 1

    #keep grabbing tweets until there are no tweets left to grab
    while len(new_tweets) > 0:
        # print "getting tweets before %s" % (oldest)

        #all subsiquent requests use the max_id param to prevent duplicates
        new_tweets = api.user_timeline(id=user_id, count=200, max_id=oldest)

        #save most recent tweets
        alltweets.extend(new_tweets)

        #update the id of the oldest tweet less one
        oldest = alltweets[-1].id - 1
    
    print str(len(alltweets)), "tweets downloaded."

    return alltweets

def main():
    global api
    consumer_key, consumer_secret, access_key, access_secret = [key.strip() for key in open("keys")]
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)


    print "Starting."
    with open(data_dir+"downloaded_users","a+") as dled_users_handle:
        downloaded_users = set([int(line.strip().split(",")[0]) for line in dled_users_handle])
    with open(data_dir+"users_to_download") as users_to_dl_handle:
        users_to_download = [int(line.strip()) for line in users_to_dl_handle if int(line.strip()) not in downloaded_users]
    try:
        for uid in users_to_download:
            try:
                alltweets = get_all_tweets(uid)
                if alltweets:
                    with open(data_dir+profiles_dir+str(uid),"w+",0) as f:
                        for tweet in alltweets:
                            decoded = tweet._json
                            f.write(json.dumps(decoded)+"\n")
                            downloaded_users.add(uid)
                        with open(data_dir+"downloaded_users","a+",0) as dled_users_handle:
                            dled_users_handle.write(str(uid)+"\n")
                else:
                    with open(data_dir+"downloaded_users","a+",0) as dled_users_handle:
                        dled_users_handle.write(str(uid)+",no_tweets\n")
                    downloaded_users.add(uid)
            except KeyboardInterrupt:
                raise
            except tweepy.TweepError, e:
                with open(data_dir+"downloaded_users","a+") as dled_users_handle:
                    dled_users_handle.write(str(uid)+",exception:"+str(e)+"\n")
                downloaded_users.add(uid)
                print "Tweepy error:",e[0]['code'],"("+e[0]['message']+")"
            except Exception, e:
                raise
    except KeyboardInterrupt:
        print "System exiting due to keyboard interrupt."
    except Exception, e:
        print "Error in main:", str(e)
        sleep(30)
        main()



if __name__ == '__main__':
    main()