import os
import requests as r
import pandas as pd
import numpy as np
import urllib.parse
from langdetect import DetectorFactory
from langdetect import detect
from nltk.sentiment.vader import SentimentIntensityAnalyzer
DetectorFactory.seed = 0
sid = SentimentIntensityAnalyzer()

# Include sanitation of query before running through functions
# place Bearer in environment variable or somewhere else secure

dummy = pd.DataFrame({'id': 0, 'text':'', 'created_at': pd.to_datetime("01/01/2001", infer_datetime_format=True), 'retweet_count': 0,
                      'reply_count': 0, 'like_count': 0, 'quote_count': 0, 'place_id': np.NaN, 'Language': '', 'Tags' : [['#', '#']],
                      'CleanText': '', 'Targeted @': [['@', '@']], 'Tweet Url': '', 'SentimentScore': 0.25}, index=[0])


def lambda_handler(rawquery, amountofruns=10):
    json = GetTweets(rawquery, amountofruns, os.environ.get('Twitter'))
    return json


def GetTweets(rawquery, amountofruns, BearerToken):
    """Main Function, returns dataframe and response code in json format"""
    dataframelist = []
    token = ""
    for i in range(amountofruns):
        if i != 0 and token == "":
            DataFrame, FinalResponse = FinaliseData(dataframelist)
            return {'Data': DataFrame, 'Response': FinalResponse}
        else:
            try:
                tries = 0
                df, token, response = TidyResponse(GetRecentTweets(rawquery, token, BearerToken))
                while int(str(response.status_code)[0]) == 5 & tries < 4:
                    tries += 1
                    df, token, response = TidyResponse(GetRecentTweets(rawquery, token, BearerToken))
                if tries >= 4:
                    DataFrame, FinalResponse = FinaliseDataError(dataframelist, "Twitter server currently not responding", response.status_code)
                    return {'Data': DataFrame, 'Response': FinalResponse}
                dataframelist.append(df)
            except Exception as e:
                print(f" GetTweets {e}")
                DataFrame, FinalResponse = FinaliseDataError(dataframelist, e)
                return {'Data': DataFrame, 'Response': FinalResponse}
    DataFrame, FinalResponse = FinaliseData(dataframelist)
    return {'Data': DataFrame, 'Response': FinalResponse}


def GetRecentTweets(rawquery, BearerToken, next_token=""):
    query = urllib.parse.quote(rawquery)
    header={'Authorization': f'Bearer {BearerToken}'}
    querystring=f'query={query}&tweet.fields=created_at,public_metrics,geo&max_results=100'
    token = f"next_token={next_token}&" if next_token != "" else ""
    response = r.get(f"https://api.twitter.com/2/tweets/search/recent?{token}{querystring}", headers=header, timeout=2)
    return response


def TidyResponse(response):
    try:
        cleaneddata = []
        token = response.json()['meta']['next_token'] if 'next_token' in response.json()['meta'] else ""
        for i in response.json()['data']:
            data = i
            details = data.pop('public_metrics')
            geo = data.pop('geo') if 'geo' in data else {'place_id': np.NaN}
            cleaned = {**data, **details, **geo}
            cleaneddata.append(cleaned)
        return (pd.DataFrame(cleaneddata), token, response)
    except Exception as e:
        print(f"tidy response, {e}")
        return (pd.DataFrame({'id': np.NaN, 'text': ''}, index=[0]), "", response)


def FinaliseData(dataframelist):
    """ return dataframe in json format and return response containing a status code and text in json """
    df = pd.concat(dataframelist)
    length = len(df.index)
    if length == 0:
        return df.to_json(orient="records"), "404, There are no recent tweets with the chosen parameters."
    df = pd.concat([df, CleanText(df["text"])], axis=1)
    df = DetectLang(df)
    df = GetSentiment(df)
    return df.to_json(orient="records"), f"200, {len(df[df['Language'] == 'en'])} English Tweets out of {len(df)} Multi-Lingual Tweets categorised"


def FinaliseDataError(dataframelist, Exception, response=0):
    """ return dataframe in json format and return response containing a status code and text in json """
    try:
        df = pd.concat(dataframelist)
    except:
        return dummy.to_json(orient="records"), f"{response.status_code}, unfortunately we ran into an error and couldn't get data before this happened, please try again with different paremeters"
    df = pd.concat([df, CleanText(df["text"])], axis=1)
    df = DetectLang(df)
    df = GetSentiment(df)
    if int(str(response.status_code)[0]) == 5:
        response = f"{response.status_code}, Twitter server currently not responding, however we managed to get {len(df[df['Language'] == 'en'])} English Tweets out of {len(df)} Multi-Lingual Tweets categorised before hand"
    else:
        response = f"500, Unknow Error encountered, however we managed to get {len(df[df['Language'] == 'en'])} English Tweets out of {len(df)} Multi-Lingual Tweets categorised before hand"
    return df.to_json(orient="records"), response    


def CleanText(x: pd.Series):
    x = x.str.strip().str.lower().str.split()
    dfs = []
    for row in range(len(x)):
        info = {'Tags': [[]], 'CleanText': "", 'Targeted @': [[]], 'Tweet Url': ""}
        for word in x[row]:
            if "@" in word:
                info['Targeted @'][0].append(word)
            elif word.startswith("https://"):
                info['Tweet Url'] = word
            else:
                if word.startswith("#"):
                    info['Tags'][0].append(word)
                    word = word.replace("#", "")
                info['CleanText'] = f"{info['CleanText']}{word} "
        for property in info.keys():
            if type(info[property]) == list:
                if len(info[property][0]) == 0:
                    info[property] = np.NaN
            elif len(info[property]) == 0:
                info[property] = np.NaN
        dfs.append(pd.DataFrame(info, index=[row]))
    return pd.concat(dfs)


def DetectLang(df):
    """ detects language of each tweet using a python port of Nakatani Shuyo’s language-detection library"""
    df["Language"] = df["CleanText"].apply(lambda x: detect(x))
    return df


def GetSentiment(df):
    df.loc[df["Language"] == "en", "SentimentScore"] = df.loc[df["Language"] == "en", "CleanText"].apply(lambda x: sid.polarity_scores(x)["compound"])
    return df



    
    
