# Static data includes video ID, publish time, title, playlist id, playlist name
import os
import math
import json
from numpy import i0
import requests
import datetime
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv
from urllib.error import HTTPError
from googleapiclient.discovery import build

load_dotenv()
API_KEY = os.getenv("API_KEY")

youtube = build('youtube', 'v3', developerKey=API_KEY)


CHANNEL_IDS = {
    "希望之聲粵語頻道": "UCCdWF5GML4ai-DVSp0Tgyxg",
    "希望之聲TV": "UCk89pEd76qutMB08hVSY49Q",
    "頭頭是道": "UCizGWTffp1z_d4oU_gpwl-Q"
}

def getAllPlaylists(channelId, PageToken=None):
    if PageToken == None:
        request = youtube.playlists().list(
            part='id, snippet',
            channelId=channelId,
            maxResults=50
        )
    else:
        request = youtube.playlists().list(
            part='id, snippet',
            channelId=channelId,
            maxResults=50,
            pageToken=PageToken
        )
    try:
        response = request.execute()
    except HTTPError as e:
        print('Error response status code : {0}, reason : {1}'.format(
            e.status_code, e.error_details))

    playlistIds = []
    playlistTitles = {}
    for i in tqdm(response['items']):
        playlistIds.append(i['id'])
        playlistTitles[i['id']] = (i['snippet']['localized']['title'])

    nextPageToken = response.get('nextPageToken', None)
    if nextPageToken is not None:
        print("nextPageToken: " + nextPageToken)
        nextPagePlaylistIds, nextPagePlaylistTitles = getAllPlaylists(
            channelId, PageToken=nextPageToken)
        playlistIds = playlistIds + nextPagePlaylistIds
        playlistTitles = playlistTitles.update(nextPagePlaylistTitles)
        return playlistIds, playlistTitles
    else:
        return playlistIds, playlistTitles

for i in ['UCCdWF5GML4ai-DVSp0Tgyxg', 'UCk89pEd76qutMB08hVSY49Q']:
    playlistID, playlistNames = getAllPlaylists(i)
    print(playlistID, playlistNames)