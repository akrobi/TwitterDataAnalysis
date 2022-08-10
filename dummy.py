import json
import pandas as pd
from textblob import TextBlob


def read_json(json_file: str)->list:
    '''json file reader to open and read json files into a list'''
    tweets_data = []
    # count = 0 #gets the first 5 tweets for now
    for tweets in open(json_file, 'r'):
        # count += 1
        tweets_data.append(json.loads(tweets))
        # if count == 5:
        #     break

    df = pd.read_json(json_file)

    return len(tweets_data), tweets_data

# globaldata = read_json("data/global_twitter_data/global_twitter_data.json")
# print(globaldata)
