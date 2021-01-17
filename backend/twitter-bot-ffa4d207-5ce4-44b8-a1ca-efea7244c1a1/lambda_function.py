import os
import random
import json
from pathlib import Path
import tweepy
import csv
import boto3
from tweepy.parsers import JSONParser
from decimal import Decimal



ROOT = Path(__file__).resolve().parents[0]


def get_tweet(tweets_file, excluded_tweets=None):
    """Get tweet to post from CSV file"""

    with open(tweets_file) as csvfile:
        reader = csv.DictReader(csvfile)
        possible_tweets = [row["tweet"] for row in reader]

    if excluded_tweets:
        recent_tweets = [status_object.text for status_object in excluded_tweets]
        possible_tweets = [tweet for tweet in possible_tweets if tweet not in recent_tweets]

    selected_tweet = random.choice(possible_tweets)

    return selected_tweet


def lambda_handler(event, context):
    client = boto3.resource('dynamodb')
    client_comprehend = boto3.client('comprehend')
    print("Get credentials")
    consumer_key = os.getenv("CONSUMER_KEY")
    consumer_secret = os.getenv("CONSUMER_SECRET")
    access_token = os.getenv("ACCESS_TOKEN")
    access_token_secret = os.getenv("ACCESS_TOKEN_SECRET")

    print("Authenticate")
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth, parser=JSONParser())

    print("Get search from csv file")
    tweets_file = ROOT / "possible_disasters.csv"
    recent_tweets = api.user_timeline()[:3]
    search = get_tweet(tweets_file)

    print(f"Search tweet: {search}")
    
    returned_tweets = api.search(search, result_type = 'top', count = 50)
    

    dynamodb = boto3.client('dynamodb')
    table = client.Table("tweets")
    print('printing table status..\n')
    print(table.table_status)
    

    
    for i in range(len(returned_tweets["statuses"])):
        tweet = returned_tweets["statuses"][i]
        lang = tweet['lang']
        if lang is None:
            continue
        if lang not in ['ar', 'hi', 'ko', 'zh-TW', 'ja', 'zh', 'de', 'pt', 'en', 'it', 'fr', 'es']:
            continue
        tweet_id = tweet["id_str"]
        text = tweet["text"]
        str_arr = search.split(", ")
        location = str_arr[0]
        disaster = str_arr[1]
        date = tweet['created_at']
        
        sentiment_data = client_comprehend.detect_sentiment(Text=text, LanguageCode=lang)
        sentiment_score = Decimal(sentiment_data["SentimentScore"]["Positive"] - sentiment_data["SentimentScore"]["Negative"]).quantize(Decimal('1.00'))
        

        table.put_item(Item= {'tweet_id': tweet_id,'location#disaster': f"{location}+{disaster}",
                                'location': location, 'disaster': disaster,
                                'date': date, 'sentiment_score': sentiment_score})

    json_str = json.dumps(returned_tweets)
    return {"statusCode": 200, "tweets": json_str}
