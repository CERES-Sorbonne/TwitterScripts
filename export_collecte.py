import os
import sys
import json

if __name__ == "__main__":
    output = 'id;text;created_at;author_id;author_name;referenced_tweet;nb_retweets;image_url;user@1;user@2;user@3;user@4;user@5'
    path = sys.argv[1]
    for tweet_file in os.listdir(path):
        with open(os.path.join(path, tweet_file), 'r', encoding='utf-8') as f:
            tweet = json.load(f)
        author_id = tweet['data']['author_id']
        id = tweet['data']['id']
        text = tweet['data']['text'].replace('\n', ' ')
        created_at = tweet['data']['created_at']
        ref_tweet = tweet['data'].get('referenced_tweets', [{'id': 'null'}])[0]['id']
        image_url = tweet['includes'].get('media', [{'url': 'null'}])[0].get('url', 'null')
        nb_retweets = tweet['data']['public_metrics']['retweet_count']
        users = []
        for index, user in enumerate(tweet['includes']['users']):
            if user['id'] == author_id:
                author_name = user['username']
            else:
                users.append(user['username'])
        for i in range(5 - len(users)):
            users.append('null')
        row = f'\n{id};"{text}";{created_at}{author_id};{author_name};{ref_tweet};{nb_retweets};{image_url};'
        output += row
        output += ";".join(users)
    with open('output.csv', 'w') as f:
        f.write(output)