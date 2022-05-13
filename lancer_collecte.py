import requests
import json
import os
import hashlib
import sys
import time
from datetime import datetime
from pathlib import Path

API_ROUTE = "https://api.twitter.com/2/"
STREAM_ROUTE = "tweets/search/stream"
RULES_ROUTE = "tweets/search/stream/rules"

# maximum size of the collect in octets
MAX_SIZE = 60073741824

params = {
    "tweet.fields": "public_metrics,referenced_tweets,created_at",
    "expansions": "author_id,in_reply_to_user_id,attachments.media_keys",
    "media.fields": "url,alt_text",
    "user.fields": "id,verified,description,name,username,created_at"
    }

def download_media(media_key=None, url=None, tags=[], **kwargs):
    if not media_key or not url:
        raise ValueError("Missing field when trying to save media")
    file_type = url.split('.')[-1]
    sha1 = None
    # download the file
    try:
        res = requests.get(url)
    except requests.RequestException:
        raise ValueError(f"There was an error when downloading the media with following url: {url}, please check your connection or url")
    
    buffer = res.content
    signature = hashlib.sha1(buffer).hexdigest()
    file_name = f"{signature}.{file_type}"

    for tag in tags:
        directory = os.path.join(ROOT_FOLDER, tag, 'media')

        with open(os.path.join(directory, file_name), 'wb') as f:
            f.write(res.content)
            
        with open(os.path.join(directory, 'sha1.json'), 'r') as f:
            sha1 = json.load(f)
        
        sha1[media_key] = file_name
        
        with open(os.path.join(directory, 'sha1.json'), 'w') as f:
            json.dump(sha1, f)
    
def handle_media(tweet, tags):
    # check if there are some media in the tweet
    if not tweet.get('includes', {}).get('media', False):
        return
    # extract the media
    media = tweet['includes']['media']
    for medium in media:
        if medium['type'] != 'photo':
            print(f"unhandled media type currently: {medium['type']}")
            continue
        
        download_media(**medium, tags=tags)

def handle_tweet(tweet):
    id = tweet['data']['id']
    
    # filter tags:
    tags = list(set([r['tag'] for r in tweet["matching_rules"]]))
    
    # save media:
    handle_media(tweet, tags)
    
    # save collected tweet in every tag
    for tag in tags:
        # create directory if not exist
        directory = os.path.join(ROOT_FOLDER, tag, 'tweets')
        with open(os.path.join(directory, f"{id}.json"), 'w', encoding='utf-8') as f:
            json.dump(tweet, f, indent=4, ensure_ascii=False)

def get_rules(s):
    """
    Allow to get all rules currently active for the collect
    """
    rules = s.get(API_ROUTE + RULES_ROUTE)
    return rules.json()

def get_tags_from_rules(rules):
    if 'data' not in rules:
        raise ValueError('You seem to have no rules configured yet, please make sure to create some')
    return list(set([r['tag'] for r in rules['data']]))

def init_rules(s, rules):
    print("initialisation des règles de collecte")
    old_rules = get_rules(s)
    rules_to_remove = []
    rules_to_add = []
    if 'data' in old_rules:
        for rule in old_rules['data']:
            # check if an old rule has to be deleted cause it's no longer in the file
            if rule['value'] not in rules.values():
                rules_to_remove.append(rule['id'])
                print("Removing old rule : " + rule['value'])
        # check if among the new_rules, some are already existing
        for tag, new_rule in rules.items():
            if new_rule not in [r['value'] for r in old_rules['data']]:
                rules_to_add.append((tag, new_rule))
                print("Adding new rule : " + new_rule)
    # if no old rules, just add all the new ones
    else:
        rules_to_add = [(tag, rule) for tag, rule in rules.items()]
    # remove old rules
    s.post(API_ROUTE + RULES_ROUTE, json={'delete': {'ids': rules_to_remove}})
    # create new
    rules_to_create = [{'tag': r[0], 'value': r[1]} for r in rules_to_add]
    res = s.post(API_ROUTE + RULES_ROUTE, json={'add': rules_to_create})
    if res.status_code == 401:
        print("Invalid token !")
        return
    if 'errors' in res.json():
        for error in res.json()['errors']:
            print(f"There was an error creating the rule {error['value']} : {error['title']}")
    print(res.json())
    print([r['value'] for r in get_rules(s)['data']])

def save_rules(rules):
    for rule in rules['data']:
        with open(os.path.join(ROOT_FOLDER, 'rules', f"{rule['id']}.json"), 'w') as f:
            json.dump(rule, f)

def generate_token():
    with open(CREDENTIALS_FILES, 'r') as f:
        return f"Bearer {json.load(f)['token']}"

def init_storages(folders=[]):
    """
    Ensure folders are properly created at the begining of the collect
    """
    rules_dir = Path(os.path.join(ROOT_FOLDER, 'rules'))
    rules_dir.mkdir(parents=True, exist_ok=True)
    for f in folders:
        media_dir = Path(os.path.join(ROOT_FOLDER, f, 'media'))
        tweet_dir = Path(os.path.join(ROOT_FOLDER, f, 'tweets'))
        media_dir.mkdir(parents=True, exist_ok=True)
        tweet_dir.mkdir(parents=True, exist_ok=True)
        if 'sha1.json' not in os.listdir(media_dir):
            with open(os.path.join(media_dir, 'sha1.json'), 'w') as f:
                json.dump({}, f)

def get_folder_size(path):
    return sum(f.stat().st_size for f in Path(path).glob('**/*') if f.is_file())

def has_free_space():
    if not MAX_SIZE:
        return True
    if get_folder_size(ROOT_FOLDER) < MAX_SIZE:
        return True
    raise OSError("The maxsize of the storage directory has been reached")

def collect(rules):
    total = 0
    s = requests.Session()
    s.headers.update({"Authorization": generate_token()})
    # update all rules
    init_rules(s, rules)
    # first ensure all folders needed are created
    online_rules = get_rules(s)
    init_storages(get_tags_from_rules(online_rules))
    # save the rules once validated by twitter with their id
    save_rules(online_rules)

    # then connect to the stream
    with s.get(API_ROUTE + STREAM_ROUTE, params=params, stream=True, timeout=5000) as resp:
        if resp.status_code != 200:
            print(f"error {resp.status_code}")
            print(resp.content)
        for line in resp.iter_lines():
            if line and has_free_space():
                data = json.loads(line.decode("utf-8"))
                if 'data' not in data:
                    print(data)
                    time.sleep(30)
                    lancer_collecte(rules)
                total += 1
                if total % 30 == 0:
                    print(f'collected {total} tweets at {str(datetime.now())} last tweet:')
                    print(data['data']['text'])
                handle_tweet(data)

if __name__ == "__main__":
    CREDENTIALS_FILES = sys.argv[1]
    ROOT_FOLDER = sys.argv[2]
    with open(os.path.join(os.path.dirname(__file__), 'rules.json'), 'r') as f:
        rules = json.load(f)
    collect(rules)

# python lancer_collecte.py ../credentials.json D:\Alie\Documents\CollectesTwitter