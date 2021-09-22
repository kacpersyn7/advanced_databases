#%%
import tweepy
import pandas as pd
import time

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth,wait_on_rate_limit=True)

username = 'MZ_GOV_PL'
text_query = 'koronawirus'
count = 100     
try:
    # Creation of query method using parameters
    tweets = tweepy.Cursor(api.search,q=text_query).items(count)

    column_names = ['id', 'author_name', 'created_at', 'text', 'hashtags', 'user_mentions']
    tweets_list = [[tweet.id, tweet.user.name, tweet.created_at, tweet.text, 
                    [x['text'] for x in tweet.entities['hashtags']], 
                    [x['name'] for x in tweet.entities['user_mentions']]] for tweet in tweets]
    # Creation of dataframe from tweets list
    # Add or remove columns as you remove tweet information
    tweets_df = pd.DataFrame(tweets_list, columns=column_names).set_index('id').explode('hashtags').explode('user_mentions')
    

except BaseException as e:
    print('failed on_status,',str(e))
    time.sleep(3)

tweets_df.to_csv('data_koronawirus.csv')