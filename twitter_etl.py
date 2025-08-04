import tweepy 
import pandas as pd
import json
from datetime import datetime
import s3fs

# API credentials (consider moving to environment variables for security)
consumer_key = '733R4I6F9WFB92GDeHu7rzQmG'
consumer_secret = '7Pxg8GLsecy200ZEvP8KonEtI4vcTbVOYkkwZYA72FjaLXu0uL'
access_token = '1782750523717750785-CjOT6QtIKNRybxp8M8p1tx0y5y4xQR'
access_token_secret = 'jbRDo4MxjpdCFN9e5ZkjqDeaMxopVbW9iKcsgHx3f5o8T'
bearer_token = 'AAAAAAAAAAAAAAAAAAAAAIDbtQEAAAAAsMTQjTxekoUZzX8OkKMCquNIGgs%3DGdX4rfTr8IdwHYuRdtbislLt5W528Sx3fXN6cmlCwKdAGb9H6d'

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