import io
import pyarrow
import tweepy
import pandas as pd
import json
from datetime import datetime
import boto3

def run_twitter_etl():
    access_key = 'w1560SVcH0OBa9SIS93B4AbqS'
    access_secret = 'c9DY4Z7EYTMmncBBZp4bIZzBbDhO14Q7jNh0QFhNXDWEQ1fvC0'
    consumer_key = '920418075471745025-JiXeI5Me6vm8EcgBWlSoH81r8S1SDiV'
    consumer_secret = 'DOHIiYFYEqzPkac0PADmIF84EftsFuQYeabBXLrUXGbun'

    #Twitter Authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key, consumer_secret)

    #Creating an API Object
    api = tweepy.API(auth)

    tweets = api.user_timeline(screen_name='@RedeCoxa',
                            count=200,
                            include_rts=False,
                            tweet_mode='extended'
                            )

    tweet_list = []

    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {'user': tweet.user.screen_name,
                        'text': text,
                        'favorite_count': tweet.favorite_count,
                        'retweet_count': tweet.retweet_count,
                        'created_at': tweet.created_at                     
                        }
        
        tweet_list.append(refined_tweet)

    df = pd.DataFrame(tweet_list)

    # Save DataFrame to parquet file
    s3 = boto3.client('s3')
    bucket_name = 'bancobari-raw-381733490365'
    file_name = 'twitter/redecoxa.parquet'

    # Save DataFrame to parquet file in memory
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)

    # Upload the file to S3
    s3.upload_fileobj(parquet_buffer, bucket_name, file_name)

run_twitter_etl()
