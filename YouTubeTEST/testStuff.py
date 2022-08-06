import analytix, calendar, datetime, json, os, requests 

import pandas as pd 
import tkinter as tk

from tqdm import tqdm
from pathlib import Path
from datetime import date
from analytix import Analytics
from dotenv import load_dotenv
from calendar import month_name
from urllib.error import HTTPError
from tkinter import ttk, filedialog
from sqlalchemy import create_engine
from tkinter.messagebox import showinfo
from google.auth.transport import Request
from tkinter.filedialog import askdirectory
from googleapiclient.discovery import build
from sqlalchemy.types import NVARCHAR, Float, Integer


client = Analytics.with_secrets("./ignore/SOHclient_secret.json")
load_dotenv()

CHANNEL_IDS = {
    "希望之聲粵語頻道": "UCCdWF5GML4ai-DVSp0Tgyxg",
    "希望之聲TV": "UCk89pEd76qutMB08hVSY49Q",
    "頭頭是道": "UCizGWTffp1z_d4oU_gpwl-Q"
}
METRICS = ['views', 'redViews', 'comments', 'likes', 'dislikes', 'videosAddedToPlaylists', 'videosRemovedFromPlaylists', 'shares', 
                'estimatedMinutesWatched', 'estimatedRedMinutesWatched', 'averageViewDuration', 'averageViewPercentage', 'annotationClickThroughRate', 
                'annotationCloseRate', 'annotationImpressions', 'annotationClickableImpressions', 'annotationClosableImpressions', 'annotationClicks', 
                'annotationCloses', 'cardClickRate', 'cardTeaserClickRate', 'cardImpressions', 'cardTeaserImpressions', 'cardClicks', 'cardTeaserClicks',
                'subscribersGained', 'subscribersLost', 'estimatedRevenue', 'estimatedAdRevenue', 'grossRevenue', 
                'estimatedRedPartnerRevenue', 'monetizedPlaybacks', 'playbackBasedCpm', 'adImpressions', 'cpm']

startDate = "2022-04-01"
endDate = "2022-04-30"

format = '%Y-%m-%d'
startDate = datetime.datetime.strptime(startDate, format).date()
endDate = datetime.datetime.strptime(endDate, format).date()

client.authorise() #This allows us to run the code for every channel depending on its token 


report = client.retrieve(
            start_date=startDate, #retrieve all data starting from 2021-12-01
            end_date=endDate,
    #  dimensions=['video'], #reztrieve data for all videos
            metrics=METRICS, #list of metrics to retrieve data for
            # max_results=200, # set maximum number of results (has to be less than 200)
            filters={'video':"wY35qgnYT-Q,eYmu86N5Ww0,tGxaXxvpxks"},
            # filters={'video':"[eYmu86N5Ww0]"},
            include_historical_data=True) 

df = report.to_dataframe()
print(df)