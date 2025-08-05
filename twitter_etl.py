import tweepy 
import pandas as pd
import json
from datetime import datetime
import s3fs
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# API credentials from environment variables
consumer_key = os.getenv('CONSUMER_KEY')
consumer_secret = os.getenv('CONSUMER_SECRET')
access_token = os.getenv('ACCESS_TOKEN')
access_token_secret = os.getenv('ACCESS_TOKEN_SECRET')
bearer_token = os.getenv('BEARER_TOKEN')

# Twitter API v2 authentication
client = tweepy.Client(
    bearer_token=bearer_token,
    consumer_key=consumer_key,
    consumer_secret=consumer_secret,
    access_token=access_token,
    access_token_secret=access_token_secret,
    wait_on_rate_limit=True
)

try:
    # Get user by username
    user = client.get_user(username='elonmusk')
    print(f"User found: {user.data.username}")
    
    # Get user's tweets
    tweets = client.get_users_tweets(
        id=user.data.id,
        max_results=10,
        exclude=['retweets'],
        tweet_fields=['created_at', 'public_metrics', 'text']
    )
    
    if tweets.data:
        print(f"Number of tweets fetched: {len(tweets.data)}")
        
        # Process tweets (moved inside try block)
        tweet_list = []
        for tweet in tweets.data:  # Use tweets.data, not tweets
            refined_tweet = {
                "user": user.data.username,  # Use user.data.username
                "text": tweet.text,  # Use tweet.text, not tweet.json["full_text"]
                'favorite_count': tweet.public_metrics['like_count'],  # API v2 structure
                'retweet_count': tweet.public_metrics['retweet_count'],  # API v2 structure
                'created_at': tweet.created_at,
            }
            tweet_list.append(refined_tweet)

        # Create DataFrame and save
        df = pd.DataFrame(tweet_list)
        df.to_csv('/Users/tarekbouhairi/Desktop/projects/twitter-airflow-data-engineering-project/elonmusk_twitter.csv', index=False)
        print("Data saved successfully!")
        
    else:
        print("No tweets found")
        
except tweepy.Unauthorized:
    print("Error: Unauthorized - Check your API credentials")
except tweepy.Forbidden:
    print("Error: Forbidden - Your app may not have the required permissions")
except Exception as e:
    print(f"Error: {e}")