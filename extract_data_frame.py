import json
from time import strftime
import pandas as pd
from textblob import TextBlob

def read_json(json_file: str) -> list:
    """
    json file reader to open and read json files into a list
    Args:
    -----
    json_file: str - path of a json file
    
    Returns
    -------
    length of the json file and a list of json
    """
    
    tweets_data = []
    for tweets in open(json_file,'r'):
        tweets_data.append(json.loads(tweets))

    return len(tweets_data), tweets_data

# the columns in the json twt data files
# columns = ["created at", "id", "id_str", "text", "source", "truncated", 
#               "in_reply_to_status_id", "in_reply_to_status_id_str", 
#               "in_reply_to_user_id", "in_reply_to_user_id_str", 
#               "in_reply_to_screen_name", "user", "coordinates", "place", 
#               "quoted_status_id", "quoted_status_id_str", "is_quote_status", 
#               "quoted_status", "retweeted_status", "quote_count", "reply_count", 
#               "retweet_count", "favorite_count", "entities", "extended_entities", 
#               "favorited", "retweeted", "possibly_sensitive", "filter_level", 
#               "lang", "matching_rules"]

class TweetDfExtractor:
    """
    this function will parse tweets json into a pandas dataframe
    
    Return
    ------
    dataframe
    """
    def __init__(self, tweets_list):
        self.tweets_list = tweets_list

    def find_statuses_count(self) -> list:
        statuses_count = []
        for tweet in self.tweets_list:
            statuses_count.append(tweet['user']['statuses_count'])

        return statuses_count
        
    def find_full_text(self) -> list:
        text = []
        for tweet in self.tweets_list:
            text.append(tweet['full_text'])

        return text
    
    
    def find_sentiments(self, text) -> list:
        # sentiment = []
        # polarity = []
        # subjectivity = []
        # for tweet in self.tweets_list:
        #     blob = TextBlob(tweet['full_text'])
        #     sentiment.append(blob.sentiment)
        #     polarity.append(sentiment.polarity)
        #     subjectivity.append(sentiment.subjectivity)
        polarity=[TextBlob(tweet).sentiment.polarity for  tweet in text] 
        subjectivity=[TextBlob(tweet).sentiment.subjectivity for tweet in text]

        # return sentiment, polarity, subjectivity
        return polarity, subjectivity

    def find_lang(self) -> list:
        lang = []
        for tweet in self.tweets_list:
            lang.append((tweet['lang']))

        return lang   

    def find_created_time(self) -> list:
        created_at = []
        for tweet in self.tweets_list:
            created_at.append((tweet['created_at']))

        return created_at
    
    def find_source(self) -> list:
        source = []
        for i in self.tweets_list:
            source.append(i['source'])
        return source

    def find_screen_name(self) -> list:
        screen_name = []
        for tweet in self.tweets_list:
            screen_name.append((tweet['user']['screen_name']))

        return screen_name

    def find_followers_count(self) -> list:
        followers_count = []
        for tweet in self.tweets_list:
            followers_count.append(tweet['user']['followers_count'])

        return followers_count

    def find_friends_count(self) -> list:
        friends_count = []
        for tweet in self.tweets_list:
            friends_count.append(tweet['user']['friends_count'])

        return friends_count

    def is_sensitive(self) -> list:
        is_sensitive = []
        for tweet in self.tweets_list:
            if 'possibly_sensitive' in tweet.keys():
                is_sensitive.append(tweet['possibly_sensitive'])
            else:
                is_sensitive.append(None)   

        return is_sensitive

    def find_favourite_count(self) -> list:
        favorite_count = []
        for tweet in self.tweets_list:
            favorite_count.append(tweet['favorite_count'])

        return favorite_count
    
    def find_retweet_count(self) -> list:
        retweet_count = []
        for tweet in self.tweets_list:
            retweet_count.append(tweet['retweet_count'])

        return retweet_count    

    def find_hashtags(self) -> list: 
        hashtags = []
        for tweet in self.tweets_list:
            hashtags.append(tweet['entities']['hashtags'])

        return hashtags

    def find_mentions(self) -> list: 
        mentions = []
        for tweet in self.tweets_list:
            mentions.append(tweet['entities']['user_mentions'])
        
        return mentions


    def find_location(self) -> list:
        location = []
        for tweet in self.tweets_list:
            if 'location' in tweet['user'].keys():
                location.append(tweet['user']['location'])
            else:
                location.append(None) 

        return location

    def find_if_quote(self) -> list: 
        if_quote = []
        for tweet in self.tweets_list:
            if_quote.append(tweet['is_quote_status'])
        
        return if_quote

           
    def get_tweet_df(self, save=False) -> pd.DataFrame:
        #required column to be generated you should be creative and add more features
        
        columns = ['created_at', 'source', 'original_text', ''''sentiment',''' 'polarity', 
                    'subjectivity', 'lang', 'favorite_count', 'retweet_count', 
                    'original_author', 'followers_count','friends_count',
                    'possibly_sensitive', 'hashtags', 'user_mentions', 'place', 'if_quote']
      
        created_at = self.find_created_time()
        source = self.find_source()
        text = self.find_full_text()
        # sentiment, polarity, subjectivity = self.find_sentiments(text)
        polarity, subjectivity = self.find_sentiments(text)
        lang = self.find_lang()
        fav_count = self.find_favourite_count()
        retweet_count = self.find_retweet_count()
        screen_name = self.find_screen_name()
        follower_count = self.find_followers_count()
        friends_count = self.find_friends_count()
        sensitivity = self.is_sensitive()
        hashtags = self.find_hashtags()
        mentions = self.find_mentions()
        location = self.find_location()
        if_quote = self.find_if_quote()

        data = zip(created_at, source, text, polarity, subjectivity, lang, fav_count, 
                    retweet_count, screen_name, follower_count, friends_count, 
                    sensitivity, hashtags, mentions, location, if_quote)
        df = pd.DataFrame(data=data, columns=columns)
        
        mysampledf = df.head(10)

        if save:
            df.to_csv('database/processed_tweet_data.csv', index=False)
            mysampledf.to_json('data/sample.json')
            print('Files Successfully Saved.!!!')
        
        return df

                
if __name__ == "__main__":
    # required column to be generated you should be creative and add more features
    columns = ['created_at', 'source', 'original_text','clean_text', 
                'sentiment','polarity','subjectivity', 'lang', 'favorite_count', 
                'retweet_count', 'original_author', 'screen_count', 
                'followers_count','friends_count','possibly_sensitive', 'hashtags', 
                'user_mentions', 'place', 'place_coord_boundaries', 'if_quote']
    _, tweet_list = read_json("data/global_twitter_data.json")

    # import pprint
    # pp = pprint.PrettyPrinter(indent=4)
    # pp.pprint(tweet_list[0])

    tweet = TweetDfExtractor(tweet_list)
    tweet_df = tweet.get_tweet_df() 

    # use all defined functions to generate a dataframe with the specified columns above
    
 