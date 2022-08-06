from __future__ import print_function
import analytix
import datetime
import json
import os
import platform
import requests
import sys

import pandas as pd
from tqdm import tqdm
from typing import Dict
from datetime import date
from colorama import Fore
from analytix import Analytics
from dotenv import load_dotenv
from urllib.error import HTTPError
from google.auth.transport import Request
from tkinter.filedialog import askdirectory
from googleapiclient.discovery import build
from sqlalchemy.types import NVARCHAR, Float, Integer
from dataclasses import dataclass, asdict, astuple, field

# Local imports
from getAllVidID import getAllVidsIDs
from channel_config import CHANNEL_IDS as CHANNEL_ID_TUPLE
from insertPlaylistsDF import get_playlist_info_for_channel
from xml.etree.ElementInclude import include
import googleapiclient.discovery, json, os, tkinter
from googleapiclient.discovery import build 
from google.oauth2 import service_account

import pandas as pd 
from pkg_resources import working_set

from tqdm import tqdm
from tkinter import N, filedialog
from dotenv import load_dotenv
from urllib.error import HTTPError
from tkinter.messagebox import showinfo
from googleapiclient.discovery import build


#importing the addtional library 
import pygsheets
import pandas as pd
import gspread_dataframe as gd
import gspread as gs
from dataclasses import field
from authorized import spreadsheet_service
from authorized import drive_service



import datetime
  
# for timezone()
import pytz
  

import pandas as pd
import numpy as np
import gspread
import df2gspread as d2g
from sqlalchemy import create_engine
import mysql.connector
import re
import sys
from time import time, sleep
import schedule


#importing some important library
import gspread
import gspread
import pprint as pp
import pandas
import numpy
from gspread_dataframe import get_as_dataframe,set_with_dataframe



#pydevd warning: Computing repr of df (DataFrame) was slow (took 0.42s)


# from tkinter.filedialog import askdirectory
# %%
# Configuration
client = Analytics.with_secrets("./ignore/SOHclient_secret.json")
load_dotenv()
API_KEY = os.getenv("API_KEY")
youtube = build('youtube', 'v3', developerKey=API_KEY)
CHANNEL_IDS = CHANNEL_ID_TUPLE[0]

METRICS = ['views', 'redViews', 'comments', 'likes', 'dislikes', 'videosAddedToPlaylists', 'videosRemovedFromPlaylists', 'shares',
           'estimatedMinutesWatched', 'estimatedRedMinutesWatched', 'averageViewDuration', 'averageViewPercentage', 'annotationClickThroughRate',
           'annotationCloseRate', 'annotationImpressions', 'annotationClickableImpressions', 'annotationClosableImpressions', 'annotationClicks',
           'annotationCloses', 'cardClickRate', 'cardTeaserClickRate', 'cardImpressions', 'cardTeaserImpressions', 'cardClicks', 'cardTeaserClicks',
           'subscribersGained', 'subscribersLost', 'estimatedRevenue', 'estimatedAdRevenue', 'grossRevenue',
           'estimatedRedPartnerRevenue', 'monetizedPlaybacks', 'playbackBasedCpm', 'adImpressions', 'cpm']
START_DATE_PROMPT = 'Data starting date (REQUIRED), \n 數據開始日期 YYYY-MM-DD : '
END_DATE_PROMPT = 'Data ending date (entern "N" for current time) \n 數據結束日期 YYYY-MM-DD : '
DATE_FORMAT = '%Y-%m-%d'
TRANSLATION = {'Video Publish Times': '影片發布時間', "PlaylistName": "欄目", "titles": "影片標題", "Impressions": "曝光次數",
               "estimatedRevenue": "你的預估收益 (USD)", "playbackBasedCpm": "CPM 以播放次數為準的每千次展示成本", "Unique viewers": "非重複觀眾人數", "averageViewDuration": "平均觀看時長",
               "averageViewPercentage": "平均觀看比例 (%)", "Average views per viewer": '每位觀眾的平均觀看次數', "dislikes": "不喜歡次數", "likes": "喜歡次數",
               "PlaylistID": "欄目網址", "Impressions click-through rate": "曝光點閱率 (%)", "shares": "分享次數", "subscribersLost": "流失的訂閱人數", "subscribersGained": "獲得的訂閱人數",
               "views": "觀看次數", "estimatedMinutesWatched": "估計觀看時間 (分鐘)", "CumulativeSubscribers": "訂閱人數", "RPM": "每千次觀看收益 (USD)",  "estimatedHoursWatched": "估計觀看時間 (小時)",
               "redViews": "YouTube Premium 觀看次數", "comments": "已新增留言", "videosAddedToPlaylists": "加到播放列表的次數", "videosRemovedFromPlaylists": "從播放列表移除的次數",
               "estimatedRedMinutesWatched": "YouTube Premium 觀看分鐘", "cardClickRate": "資訊卡點擊率", "cardTeaserClickRate": "資訊卡前導文字點擊率", "thumbnailURL": "影片圖片網址",
               "cardImpressions": "資訊卡曝光次數", "cardTeaserImpressions": "資訊卡前導文字曝光次數", "cardClicks": "資訊卡點擊次數", "cardTeaserClicks": "資訊卡前導文字點擊次數",
               "estimatedAdRevenue": "預估YouTube 廣告收益 (USD)", "grossRevenue": "DoubleClick 和Google 售出的廣告收益 (USD)", "estimatedRedPartnerRevenue": "您的YouTube Premium收益", "monetizedPlaybacks": "估計營利播放次數",
               "cpm": "CPM  每千次展示成本", "adImpressions": "廣告曝光次數", "videoURL": "影片網址", "video": "影片ID", "channel": "頻道", "channelId": "頻道ID", 'dataStartDate': '數據開始日期', 'dataEndDate': '數據結束日期'}

# @dataclass
# class selection:
#     start_date: str = input('Data starting date (REQUIRED), 數據開始日期 YYYY-MM-DD : ').strip().upper()
#     end_date: str = input('Data ending date (entern "N" for current time) 數據結束日期 YYYY-MM-DD : ').strip().upper()
#     publish_date_before: str = ""
#     publish_date_after: str = ""
#     channels: list[str] = field(default_factory=list)

# %%

#trying to access the googlesheet
gc=gspread.service_account("credential.json")
#getting the link of the google sheet
spreadsheet_link1=input("Please paste the spreadsheet link: ")
spreadsheet_links=str(spreadsheet_link1)
#spreadsheet_Id=re.split("/",spreadsheet_links)[5]

sheetkey=re.split("/",spreadsheet_links)[5]
sh1=gc.open_by_key(sheetkey)
google_sheet_name=sh1.title

#setting the ranges
ranges="A1:BA"

s_range=sh1.worksheet("表格資料").get(str(ranges))



#reading a worksheet and create it if it doesn't exist
worksheet_title="表格資料"
try:
    worksheet=sh1.worksheet(worksheet_title)
except gspread.WorksheetNotFound:
    worksheet=sh1.add_worksheet(title=worksheet_title,rows=1000,cols=1000)
df_read=get_as_dataframe(worksheet)

def selections():
    start_date = datetime.datetime.strptime(
        input(START_DATE_PROMPT).strip(), DATE_FORMAT).date()
    end_date = input(END_DATE_PROMPT).strip().upper()
    publish_date_before = input(
        "Videos published before (enter 'N' for no limit) \n 影片發佈於此日期之前 (N 為無限制) (YYYY-MM-DD): ")
    publish_date_after = input(
        "Videos published after (enter 'N' for no limit) \n 影片發佈於此日期之後 (N 為無限制) (YYYY-MM-DD): ")

    save_location = None
    if platform.system() == "Darwin" or platform.system() == "Windows":
        print(platform.system())
        try:
            save_location = askdirectory(
                title="Please choose where you would like to save the file",
                initialdir=os.getcwd()
            )
        except:
            save_location = input("File path to save to, 儲存檔案位置: ")

    print("Select numbers from one or more channels 請選擇頻道: \n ")
    channel_list = []
    for index, val in enumerate(CHANNEL_IDS.keys()):
        print(f"{Fore.GREEN}{index} {Fore.BLUE}{val}")
        channel_list.append(val)

    selected_channels = input(
        f"{Fore.RESET}Separate channel numbers by comma: ")
    selected_channels = selected_channels.strip().split(",")
    for index, val in enumerate(selected_channels):
        try:
            selected_channels[index] = channel_list[int(val.strip())]
        except ValueError:
            print("Only integers are allowed for channel selections")
            raise

    selections = {
        "start_date": start_date,
        "end_date": end_date,
        "published_date_before": publish_date_before,
        "published_date_after": publish_date_after,
        "save_location": save_location,
        "selected_channels": selected_channels
    }

    return selections


# %%
@dataclass
class retriever:
    """
    A class used to retrieve YouTube video data and associated playlist data

    Attributes
    ----------
    channel_name: str
        name of YouTube channel
    channel_id: str
        channel ID (found in URL)
    start_date: str
        Data start date
    end_date: str
        Data end date, defaults to current date of program running if not provided
    publish_date_before: str = "N", optional
        "Video published before" filter. Defaults to "N" meaning no filter
    publish_date_after: str = "N", optional
        "Video published after" filter. Defaults to "N" meaning no filter
    translation: dict, optional 
        Translation dictionary for DataFrame columns. 
    df: Pandas DataFrame object 
        Defaults to an Empty object. Created by the get_all_data method.
    dtypedict: dict
        Data type dictionary. Initiated by map_types method. 

    Methods
    ----------
    config()
        Uses OAuth tokens to enable access for Analytix package to retrieve data.
    get_all_data()
        Retrieves all data including and sets self.df to the DataFrame.  
    get_200_vids_data()
        Retrieves 200 most viewed videos' data in the time-range. 
    insert_channel()
        Inserts channel and channel name into self.df.
    insert_dates()
        Inserts data start date & data end date into self.df.
    insert_playlists()
        Inserts playlist data and playlist names into self.df. 
    calc_rpm()
        Calculate RPM (Revenue Per Millen), a measure of how much money in USD earned per 1000 views. 
        This is calculated by dividing estimated revenue by (views x 1000). A new column called RPM is inserted into self.df.
    calculate_hours_watched()
        Calculates estimated hours watched by dividing estimated minutes watched by 60 and creates a new DataFrame column. 
    convert_to_minutes(col_name: str)
        Converts a DataFrame column to minutes, then replaces the original column with the converted values. 
        Average view duration is usually calculated this way. 
    calculate_cumulative_subscribers()
        Calculates cumulative subscribers for a video by subtracting subscribers lost from subscribers gained. 
    get_vid_url()
        Creates video URL from video ID and appends to a new column in self.df.
    get_vid_thumbnail_url()
        Gets video thumbnail URL from video ID and adds a new column in self.df.
    get_vid_title_publish_time()
        Retrieves video title and publish time and inserts values to new columns in self.df.
    translate_column_names()
        Translates column names in self.df to Chinese using self.translation.
    map_types()
        Maps types in the DataFrame. Should be used before outputting DataFrame to SQL database.
    return_dataframe()
        Returns self.df.

    """

    channel_name: str
    channel_id: str
    start_date: str
    end_date: str = date.today()
    publish_date_before: str = "N"
    publish_date_after: str = "N"
    translation: Dict[str, str] = field(default_factory=lambda: TRANSLATION)

    def __post_init__(self):

        self.channel_id = CHANNEL_IDS.get(self.channel_id)
        if self.end_date != "N" and self.end_date != "":
            self.end_date = datetime.datetime.strptime(
                self.end_date, DATE_FORMAT).date()
        else:
            self.end_date = date.today()

        if (self.channel_id == None and self.channel_name is not None):
            self.channel_id = CHANNEL_IDS.get(self.channel_name)

        self.config()

    def config(self):
        """Uses OAuth tokens to enable access for Analytix package to retrieve data."""
        print("Configuring... ")

        if not os.path.exists(f"./ignore/Tokens/{self.channel_name}"):
            os.makedirs(f'ignore/Tokens/{self.channel_name}')

        # This allows us to run the code for every channel depending on the user selection (generates a token for every channel so that we don't need to sign in everytime)
        client.authorise(f"./ignore/Tokens/{self.channel_name}")
        print(
            f"Channel: {Fore.MAGENTA}{self.channel_name} {Fore.YELLOW}{self.channel_id}{Fore.RESET}")

    def get_all_data(self):
        """
        Retrieves all data including and sets self.df to the DataFrame.

        Returns
        -------
        DataFrame object 
            A DataFrame of all the data
        """
        reportDF = self.get_200_vids_data()
        getAllVidsIDs(self.channel_id)

        df_videoID = pd.read_csv(
            f"./StaticData/{self.channel_name}-allVidIDs.csv")
        video_ids = set(df_videoID["VideoID"])
        video_ids_200 = set(reportDF['video'])

        reportDF = reportDF.set_index('video')

        # If there is no video publish date specified
        if self.publish_date_before == 'N' or self.publish_date_after == 'N':
            # gets the rest of the video IDs that need to have data retrieved
            video_ids = video_ids - video_ids_200
        else:
            # Gets the video_ids that already have data AND belong to the publish time interval
            alreadyRetrievedVidIds = video_ids.intersection(video_ids_200)
            # Gets rows that don't belong to publishing time interval
            unneededVidIds = list(video_ids_200 - alreadyRetrievedVidIds)
            # Removes rows that have the video Ids from a different publishing time interval
            reportDF = reportDF.drop(index=unneededVidIds)
            # rest of video Ids that need to have data retrieved for
            video_ids = video_ids - alreadyRetrievedVidIds

        reportDF = reportDF.reset_index()
        print(f"{Fore.CYAN} Retreiving data for 36 columns... ")
        for _, val in enumerate(tqdm(video_ids)):
            try:
                report = client.retrieve(
                    start_date=self.start_date,
                    end_date=self.end_date,
                    metrics=METRICS,
                    filters={'video': val},
                    include_historical_data=True)

                df2 = report.to_dataframe()  # converts results to dataframe object
                df2['video'] = val
            except requests.exceptions.Timeout:
                print("Timeout occurred")
            except:
                print(
                    f"{Fore.RED} Video {val} was not included in the report due to an unknown error {Fore.RESET}")

        reportDF = reportDF.reset_index()
        print(f"{Fore.RESET}")
        self.df = reportDF
        self.insert_channel()
        self.insert_dates()
        self.insert_playlists()
        self.get_vid_title_publish_time()
        self.get_vid_url()
        self.get_vid_thumbnail_url()
        self.calc_rpm()
        self.calculate_hours_watched()
        self.convert_to_minutes("averageViewDuration")
        self.calculate_cumulative_subscribers()
        self.df = self.df.set_index("video")
        return self.df

    def get_single_vid_data(self):
        pass

    def get_200_vids_data(self):
        """
        Retrieves 200 most viewed videos' data in the time-range. 

        Returns
        -------
        DataFrame
            A DataFrame object of 200 most viewed videos and their data in the given date range. 
        """
        report = client.retrieve(start_date=self.start_date,  # data starting date
                                 end_date=self.end_date,
                                 # retrieve data for all videosFF
                                 dimensions=['video'],
                                 metrics=METRICS,  # list of metrics to retrieve data for
                                 # set maximum number of results (has to be less than 200)
                                 max_results=200,
                                 # required parameter. sorts by views in descending order (- means descending)
                                 sort_options=['-views'],
                                 include_historical_data=True)

        return report.to_dataframe()

    def insert_channel(self):
        """Inserts channel and channel name into self.df."""
        print(f"{Fore.BLUE} Inserting channel names... {Fore.RESET}")
        self.df['channel'] = self.channel_name
        self.df['channelId'] = self.channel_id

    def insert_dates(self):
        """Inserts data start date & data end date into self.df."""
        print(f"{Fore.BLUE} Inserting data start dates... {Fore.RESET}")
        self.df['dataStartDate'] = self.start_date
        self.df['dataEndDate'] = self.end_date

    def insert_playlists(self):
        """Inserts playlist data and playlist names into self.df. """
        print(f"{Fore.RESET}")
        playlist_lookup, playlist_name_lookup = get_playlist_info_for_channel(
            self.channel_id)
        playlist_ids = []
        playlist_names = []
        print(f"{Fore.BLUE} Inserting playlists and playlist names... {Fore.RESET}")
        for i in self.df['video']:
            p_id = playlist_lookup.get(i, "")
            p_name = playlist_name_lookup.get(i, "NA")
            playlist_ids.append(p_id)
            playlist_names.append(p_name)
        self.df['PlaylistID'] = playlist_ids
        self.df["PlaylistName"] = playlist_names

    def calc_rpm(self):
        """Calculate RPM (Revenue Per Millen), a measure of how much money in USD earned per 1000 views. 
        This is calculated by dividing estimated revenue by (views x 1000). A new column called RPM is inserted into self.df."""
        # RPM = how much $ earned per 1,000 views
        print(f"{Fore.BLUE} Calculating RPM (Revenue Per Mille)... {Fore.RESET}")
        self.df['RPM'] = (self.df['estimatedRevenue'] /
                          self.df['views']) * 1000

    def calculate_hours_watched(self):
        """Calculates estimated hours watched by dividing estimated minutes watched by 60 and creates a new DataFrame column. """
        print(f"{Fore.BLUE} Calculating estimated hours watched... {Fore.RESET}")
        self.df['estimatedHoursWatched'] = self.df['estimatedMinutesWatched'] / 60

    def convert_to_minutes(self, col_name: str):
        """
        Converts a DataFrame column to minutes, then replaces the original column with the converted values. 
        Average view duration is usually calculated this way. 

        Parameters
        ----------
        col_name: str
            Column name in self.df intended for conversion to minutes. 
        """
        print(f"{Fore.BLUE} Converting {col_name} column to minutes... {Fore.RESET}")
        converted = []
        for i in self.df[col_name]:
            properFormat = str(datetime.timedelta(seconds=int(i)))
            converted.append(properFormat)
        self.df[col_name] = converted

    def calculate_cumulative_subscribers(self):
        """Calculates cumulative subscribers for a video by subtracting subscribers lost from subscribers gained. """
        print(f"{Fore.BLUE} Calculating cumulative subscribers... {Fore.RESET}")
        self.df['CumulativeSubscribers'] = self.df['subscribersGained'] - \
            self.df['subscribersLost']

    def get_vid_url(self):
        """Creates video URL from video ID and appends to a new column in self.df."""
        print(f"{Fore.BLUE} Getting video URLs... {Fore.RESET}")
        self.df['videoURL'] = "https://www.youtube.com/watch?v=" + \
            self.df['video'].astype(str)

    def get_vid_thumbnail_url(self):
        """Gets video thumbnail URL from video ID and adds a new column in self.df."""
        print(f"{Fore.BLUE} Retrieving video thumbnail URLs... {Fore.RESET}")
        self.df[
            'thumbnailURL'] = f"https://img.youtube.com/vi/{self.df['video'].astype(str)}/0.jpg"

    def get_vid_title_publish_time(self):
        """Retrieves video title and publish time and inserts values to new columns in self.df."""
        titles = []
        publishtime = []
        print(f"{Fore.BLUE} Retrieving video titles and publish time... {Fore.RESET}")
        for i in self.df['video']:
            request = youtube.videos().list(
                part='snippet',
                id=i)
            try:
                response = request.execute()

                # print(response)
                if response['items'] != []:
                    title = response["items"][0]['snippet']['localized']['title']
                    Ptime = response["items"][0]['snippet']['publishedAt']

                    titles.append(title)
                    Ptime = datetime.datetime.strptime(
                        Ptime[:10], "%Y-%m-%d").strftime("%m/%d/%Y")
                    publishtime.append(Ptime)
                else:
                    titles.append("")
                    publishtime.append("")
            except HTTPError as e:
                print('Error response status code : {0}, reason : {1}'.format(
                    e.status_code, e.error_details))
                titles.append("")
                publishtime.append("")

        print(f"{Fore.BLUE} Inserting YouTube titles and publish time... {Fore.RESET}")
        self.df['titles'] = titles
        self.df['Video Publish Times'] = publishtime

    def translate_column_names(self):
        """Translates column names in self.df to Chinese using self.translation."""
        print(f"{Fore.BLUE} Translating column names to Chinese... {Fore.RESET}")
        self.df = self.df.rename(columns=self.translation)

    def map_types(self):
        """Maps types in the DataFrame. Should be used before outputting DataFrame to SQL database."""
        print(f"{Fore.BLUE} Converting column types... {Fore.RESET}")
        self.dtypedict = {}
        for i, j in zip(self.df.columns, self.df.dtypes):
            if "object" in str(j):
                self.dtypedict.update({i: NVARCHAR(length=255)})
            if "float" in str(j):
                self.dtypedict.update({i: Float(precision=2, asdecimal=True)})
            if "int" in str(j):
                self.dtypedict.update({i: Integer()})

    def return_dataframe(self):
        """Returns self.df."""
        return self.df


# %%
def main():
    global selections
    selections = selections()
    channels_selected = selections['selected_channels']
    start_date = selections["start_date"]
    end_date = selections["end_date"]
    df = pd.DataFrame()
    filename = ""
    save_location = selections["save_location"]
    for i in tqdm(channels_selected):
        print(f"{Fore.RESET} Retrieving data for {Fore.GREEN} {i} {Fore.RESET}")
        filename = filename + str(i) + "_"
        channel = retriever(i, CHANNEL_IDS.get(i), start_date, end_date,
                            selections["published_date_before"], selections["published_date_after"])
        channel.get_all_data()
        channel.translate_column_names()
        channel_df = channel.return_dataframe()
        df = pd.concat([df, channel_df])

    filename = filename + f"{start_date}-{end_date}.csv"
    if save_location == None or save_location == "":
        if os.path.exists(save_location):
            df.to_csv(os.path.join(save_location, filename))
            sys.exit(
                f"{Fore.RED}{filename}{Fore.RESET} was saved to {Fore.BLUE}{os.getcwd()}{Fore.RESET}")

    df.to_csv(filename)
    print(f"{Fore.RED}{filename}{Fore.RESET} was saved to {Fore.BLUE}{os.getcwd()}{Fore.RESET}")


def get_dataframe(dataframe):
    print(dataframe.head())

def create():
    spreadsheet_details = {
        'properties': {
            'title': '表格資料'
        }
    }
    sheet = spreadsheet_service.spreadsheets().create(body=spreadsheet_details, fields='spreadsheetId').execute()
    sheetId = sheet.get('spreadsheetId')
    # print('Spreadsheet ID: {0}'.format(sheetId))
    email = input("Please enter your email address: ")
    permission1 = {
        'type': 'user',
        'role': 'writer',
        'emailAddress': email
    }
    drive_service.permissions().create(fileId=sheetId, body=permission1).execute()
    # print(sheetId)
    url = 'https://docs.google.com/spreadsheets/d/' + sheetId + '/edit#gid=0'
    print("A new spreadsheet has been created for you. Here's the link: {0}".format( url ))
    return sheetId

# having another function to get the columns name
def get_colName(df):
    #field_names = [i[0] for i in mycursor.description]+ ["data_refresh_time"]
    field_names = df.columns.values.tolist()
    column = []
    column.append(field_names)
    print(column)
    return column
    
#having another function to write columns name
def write_colName(df):
    spreadsheet_id = spreadsheet_Id
    values = get_colName(df)
    value_input_option = 'RAW'
    body = {
        'values': values
    }
    
        
    result = spreadsheet_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range=range_name,
        valueInputOption=value_input_option, body=body).execute()
    #return result

#creating another function to exporting it



def export_pandas_df_to_sheets(spreadsheet_id, df):
    # if new_gs == 'N':
    #     sclear = input("Do you want to clear the sheet? Y or N ").upper()
    #     if sclear == 'Y':
    #         clear()
    #     elif sclear == 'N':
    #         pass
    # clear()
    # column_names=get_colName(df)
    print("attention")
    write_colName(df)


    column_names=get_colName(df)
    #write_colName()
    try:

        df.columns = column_names
        # renaming the column names
        df.columns=['頻道','影片 ID','影片標題','欄目','影片發布時間','觀看次數','觀看時間 (小時)','訂閱人數','曝光次數','廣告曝光次數','已新增留言','鏈接']
        
    except:
        print("columns name error")

    print(column_names)
    print(df.columns)
     
    #filling the missing values
    df.fillna('', inplace=True)
    

    body = {
        'values': df.values.tolist()
    }
  
    result = spreadsheet_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, body=body, valueInputOption='RAW', range=data_range).execute()
    print('{0} cells updated.'.format(result.get('updatedCells')))

























df=df_read





#new_gs = input("Do you want to create a new spreadsheet? Y or N ").upper()
new_gs = "N"

if new_gs == 'Y':
    spreadsheet_Id = create()
    range_name = '表格資料!A1'
    data_range = '表格資料!A2'
elif new_gs == 'N':
    #spreadsheet_link='https://docs.google.com/spreadsheets/d/1npclGuRZ1opu2wX6KDgUsfm9dC3Y0nVPvOXsudNhtAc/edit#gid=1977221911'
    #spreadsheet_link=input("Please paste the spreadsheet link: ")
    spreadsheet_linkss=spreadsheet_links
    spreadsheet_Id=re.split("/",spreadsheet_linkss)[5]
    #spreadsheet_Id = input("Please enter your spreadsheet ID: ")
    
    #sheet_cell = input("Do you want to start from 表格資料 A1? Y or N ").upper()
    sheet_cell = "N"
    

    if sheet_cell == 'Y':
        range_name = '表格資料!A1'
        data_range = '表格資料!A2'
    elif sheet_cell == 'N':
        #print("If not, please define your starting point.")
        #sheet_name = input("Please enter your sheet name: ")
        sheet_name = "表格資料"
        #cell = input("Please enter your cell number (e.g. A1): ").upper()
        cell = "A1"


        range_name = sheet_name + '!' + cell
        n_cell = int(re.split('(\d+)',cell)[1])+1
        d_cell = re.split('(\d+)',cell)[0] + str(n_cell)
        data_range = sheet_name + '!' + d_cell

        export_pandas_df_to_sheets(spreadsheet_Id, df)


        #print("Error")
        

        # print(df.head())
        # # df = df
        # #df['data_refresh_time'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # for col in df.columns:
        #     if df[col].dtype == 'datetime64[ns]':
        #         df = df.astype({col: 'string'})

        # for col in df.columns:
        #     if df[col].dtype == 'datetime64[ns]':
        #         df = df.astype({col: 'string'})



if __name__ == '__main__':

    main()

# FIXME The video published before & after filter does not seem to work (should only retrieve the videos published during the date range).
# TODO/TOTHINK: DIFFERENT CHANNELS WILL REQUIRE AUTOMATICALLY SIGNING IN MULTIPLE TIMES
# TODO: selection menu that can be navigated by arrows or by typing number, after initial selection
# TODO: use parallel programming to utilize multiple cores of the computer for: retrieving video playlists, retrieving all video IDs
# display menu without already selected item, if timeout then proceeds
# %%
