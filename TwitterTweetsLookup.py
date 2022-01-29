import requests as r
import pandas as pd
import numpy as np
import urllib.parse

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

def GetTweets(rawquery, amountofruns):
    """Main Function, returns dataframe and response code in json format"""
    dataframelist = []
    token = ""
    for i in range(amountofruns):
        if i != 0 & token == "":
            DataFrame, FinalResponse = finaliseData(dataframelist)
            return {'Data': DataFrame, 'Response': FinalResponse}
        else:
            try:
                tries = 0
                df, token, response = TidyResponse(GetRecentTweets(rawquery, token))
                while int(str(response.status_code)[0]) == 5 & tries < 4:
                    tries += 1
                    df, token, response = TidyResponse(GetRecentTweets(rawquery, token))
                if tries >= 4:
                    DataFrame, FinalResponse = finaliseDataError(dataframelist, "Twitter server currently not responding", response.status_code)
                    return {'Data': DataFrame, 'Response': FinalResponse}
                dataframelist.append(df)
            except Exception as e:
                print(e)
                DataFrame, FinalResponse = finaliseDataError(dataframelist, e)
                return {'Data': DataFrame, 'Response': FinalResponse}
    DataFrame, FinalResponse = finaliseData(dataframelist)
    return {'Data': DataFrame, 'Response': FinalResponse}

def finaliseData(dataframelist):
    # return dataframe in json format and return response containing a status code and text in json
    df = pd.concat(dataframelist)
    length = len(df.index)
    if length == 0:
        
    
def finaliseDataError(dataframelist, Exception, response=0):
    pass


def main(rawquery):
    json = GetTweets(rawquery, 10)
    return json
    