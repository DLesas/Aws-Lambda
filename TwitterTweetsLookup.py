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


# Include sanitation of query before running through functions
# place Bearer in environment variable or somewhere else secure


def GetRecentTweets(rawquery, next_token=""):
    query = urllib.parse.quote(rawquery)
    header={'Authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAAL0gWwEAAAAAI%2FO2HFba9gEFrS4cUxbjSHtqSG0%3D9G658TlsZLrNl2PSNxJZJBKlvhqeQMHYm1NcveXawgzHPvrbJT'}
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
            cleaneddata.appened(cleaned)
        return (pd.DataFrame(cleaneddata), token, response)
    except:
        return (pd.DataFrame({'id': np.NaN, 'text': np.NaN}, index=[0]), "", response)
    

def DetectLang(df):
    """ detects language of each tweet using a python port of Nakatani Shuyo’s language-detection library"""
    df["Language"] = df["CleanText"].apply(lambda x: detect(x))
    return df


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



def GetSentiment(df):
    df.loc[df["Language"] == "en", "SentimentScore"] = df.loc[df["Language"] == "en", "CleanText"].apply(lambda x: sid.polarity_scores(x)["compound"])
    return df


def FinaliseData(dataframelist):
    """ return dataframe in json format and return response containing a status code and text in json """
    df = pd.concat(dataframelist)
    length = len(df.index)
    if length == 0:
        return df.to_json(orient="records"), 404, "There are no recent tweets with the chosen parameters."
    df = pd.concat([df, CleanText(df["text"])], axis=1)
    df = DetectLang(df)
    df = GetSentiment(df)
    return df.to_json(orient="records"), 200, f"{len(df[df['Language'] == 'en'])} English Tweets out of 1000 Multi-Lingual Tweets categorised"
    
      
def FinaliseDataError(dataframelist, Exception, response=0):
    pass


def GetTweets(rawquery, amountofruns=10):
    """Main Function, returns dataframe and response code in json format"""
    dataframelist = []
    token = ""
    for i in range(amountofruns):
        if i != 0 & token == "":
            DataFrame, code, FinalResponse = FinaliseData(dataframelist)
            return {'Data': DataFrame, 'Status Code': code, 'Response': FinalResponse}
        else:
            try:
                tries = 0
                df, token, response = TidyResponse(GetRecentTweets(rawquery, token))
                while int(str(response.status_code)[0]) == 5 & tries < 4:
                    tries += 1
                    df, token, response = TidyResponse(GetRecentTweets(rawquery, token))
                if tries == 3:
                    DataFrame, code, FinalResponse = FinaliseDataError(dataframelist, "Twitter server currently not responding", response.status_code)
                    return {'Data': DataFrame, 'Status Code': code, 'Response': FinalResponse}
                dataframelist.append(df)
            except Exception as e:
                DataFrame, code, FinalResponse = FinaliseDataError(dataframelist, e)
                return {'Data': DataFrame, 'Status Code': code, 'Response': FinalResponse}
    DataFrame, code, FinalResponse = FinaliseData(dataframelist)
    return {'Data': DataFrame, 'Status Code': code, 'Response': FinalResponse}
    
    
def lambda_handler(rawquery):
    json = GetTweets(rawquery)
    return json
    
    