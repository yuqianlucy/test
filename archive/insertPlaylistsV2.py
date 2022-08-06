# %%
# IMPORTS
from __future__ import print_function

import gspread
import json
import os
import re

from tqdm import tqdm
from tkinter import *
from dotenv import load_dotenv
from urllib.error import HTTPError
from googleapiclient.discovery import build
from gspread_dataframe import get_as_dataframe
from authorized import drive_service, spreadsheet_service

load_dotenv()
API_KEY = os.getenv("API_KEY")

youtube = build('youtube', 'v3', developerKey=API_KEY)

CHANNEL_IDS = {
    "希望之聲粵語頻道": "UCCdWF5GML4ai-DVSp0Tgyxg",
    "希望之聲TV": "UCk89pEd76qutMB08hVSY49Q",
    "頭頭是道": "UCizGWTffp1z_d4oU_gpwl-Q"
}

CHANNEL_NAMES = {
    "UCCdWF5GML4ai-DVSp0Tgyxg": "希望之聲粵語頻道",
    "UCk89pEd76qutMB08hVSY49Q": "希望之聲TV",
    "UCizGWTffp1z_d4oU_gpwl-Q": "頭頭是道"
}

# trying to access the googlesheet
gc = gspread.service_account("credential.json")
# getting the link of the google sheet
spreadsheet_link1 = input("Please paste the spreadsheet link: ")
spreadsheet_links = str(spreadsheet_link1)

sheetkey = re.split("/", spreadsheet_links)[5]
sh1 = gc.open_by_key(sheetkey)
google_sheet_name = sh1.title

# setting the ranges
ranges = "A1:BA"

s_range = sh1.worksheet("表格資料").get(str(ranges))

# reading a worksheet and create it if it doesn't exist
worksheet_title = "表格資料"
try:
    worksheet = sh1.worksheet(worksheet_title)
except gspread.WorksheetNotFound:
    worksheet = sh1.add_worksheet(title=worksheet_title, rows=1000, cols=1000)

# Get some columns back out
df_read = get_as_dataframe(worksheet)


# %%

# ALL FUNCTIONS


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

    print("Retrieving playlist items and titles... ")
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


def getVidsForSinglePlaylist(playlistId, vid_playlists={}, PageToken=None, APIkey=API_KEY):
    # url = ""

    if PageToken == None:
        # url = f"https://www.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults=50&playlistId={playlistId}&key={APIkey}"

        request = youtube.playlistItems().list(
            part="contentDetails",
            maxResults=50,
            playlistId=playlistId
        )
    else:
        # url = f"https://www.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults=50&playlistId={playlistId}&key={APIkey}&pageToken={PageToken}"
        request = youtube.playlistItems().list(
            part="contentDetails",
            maxResults=50,
            playlistId=playlistId,
            pageToken=PageToken
        )

    response = request.execute()
    # json_url = requests.get(url)
    # data = json.loads(json_url.text)

    moreThanOnePlaylist = []
    for j in response['items']:
        ID = j['contentDetails']['videoId']
        initial_val = vid_playlists.get(ID, None)
        if initial_val == None:
            vid_playlists[ID] = playlistId
        else:
            print(f"A video {ID} belongs to more than one playlist ")
            # moreThanOnePlaylist.append(ID)

            if type(initial_val) is list:
                initial_val.append(playlistId)
                vid_playlists[ID] = initial_val
            else:
                vid_playlists[ID] = [initial_val, playlistId]

    # print("The following videos belong to more than one playlist: ")
    # print(moreThanOnePlaylist)

    nextPageToken = response.get('nextPageToken', None)
    if nextPageToken != None:
        return getVidsForSinglePlaylist(playlistId, vid_playlists=vid_playlists, PageToken=nextPageToken)
    else:
        return vid_playlists


def get_dataframe(dataframe):
    print(dataframe.head())


def create():
    spreadsheet_details = {
        'properties': {
            'title': '表格資料'
        }
    }
    sheet = spreadsheet_service.spreadsheets().create(
        body=spreadsheet_details, fields='spreadsheetId').execute()
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
    print(
        "A new spreadsheet has been created for you. Here's the link: {0}".format(url))
    return sheetId

# having another function to get the columns name


def get_colName(df):
    #field_names = [i[0] for i in mycursor.description]+ ["data_refresh_time"]
    field_names = df.columns.values.tolist()
    column = []
    column.append(field_names)
    print(column)
    return column

# having another function to write columns name


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
    # return result


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

    column_names = get_colName(df)
    # write_colName()
    try:

        df.columns = column_names
        # renaming the column names
        df.columns = ['頻道', '影片 ID', '影片標題', '欄目', '影片發布時間', '觀看次數',
                      '觀看時間 (小時)', '訂閱人數', '曝光次數', '廣告曝光次數', '已新增留言', '鏈接']

    except:
        print("columns name error")

    print(column_names)
    print(df.columns)

    # filling the missing values
    df.fillna('', inplace=True)

    body = {
        'values': df.values.tolist()
    }

    result = spreadsheet_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, body=body, valueInputOption='RAW', range=data_range).execute()
    print('{0} cells updated.'.format(result.get('updatedCells')))


df = df_read

PLAYLIST_IDS = []
PLAYLIST_NAMES = {}
VIDS_PLAYLISTS = {}

# Retrieve all playlist Ids and playlist names for all channels
for channelId in CHANNEL_NAMES.keys():
    playlistIds, playlistTitles = getAllPlaylists(channelId)
    PLAYLIST_IDS.extend(playlistIds)
    PLAYLIST_NAMES.update(playlistTitles)

print("Retrieving all videos IDs in all playlist IDs... ")
for pId in tqdm(PLAYLIST_IDS):
    VIDS_PLAYLISTS.update(getVidsForSinglePlaylist(pId))

with open('playlist_videos.json', 'w', encoding='utf-8') as f:
    json.dump(VIDS_PLAYLISTS, f, ensure_ascii=False, indent=4)

playlist_ids = []
playlist_names = []

print("Retrieving playlists for all videos in spreadsheet... ")

sheet_videoIDs = ""
if '影片 ID' in df.columns:
    sheet_videoIDs = df['影片 ID']
elif '影片ID' in df.columns:
    sheet_videoIDs = df['影片ID']
elif '影片' in df.columns:
    sheet_videoIDs = df['影片']
elif 'Video ID' in df.columns:
    sheet_videoIDs = df['Video ID']
elif 'Video' in df.columns:
    sheet_videoIDs = df['Video']

print(sheet_videoIDs)

for vidID in tqdm(sheet_videoIDs):
    if vidID not in VIDS_PLAYLISTS.keys():
        print(f"Missing playlist value for {vidID}")
        playlist_ids.append("")
        playlist_names.append("")
    else:
        playlist_id = VIDS_PLAYLISTS[vidID]

        if type(playlist_id) == str:
            playlist_ids.append(playlist_id)
            playlist_names.append(PLAYLIST_NAMES[playlist_id])
        elif type(playlist_id) == list:
            print(f"A video {vidID} belongs to more than one playlist ")
            playlist_id_value = ""
            playlist_name_value = ""
            for i in playlist_id:
                playlist_id_value = playlist_id_value + str(i) + ", "
                playlist_name_value = playlist_name_value + \
                    str(PLAYLIST_NAMES[i]) + ", "

            playlist_ids.append(playlist_id_value)
            playlist_names.append(playlist_name_value)
        else:
            print(
                f"Error! Playlist ID for {vidID} is invalid. Playlist ID is invalid type {type(playlist_id)} ")
            playlist_ids.append("")
            playlist_names.append("")

df['欄目'] = playlist_names
df['欄目 ID'] = playlist_ids

df.drop(df.filter(regex="Unname"), axis=1, inplace=True)

get_dataframe(df)
get_colName(df)

#new_gs = input("Do you want to create a new spreadsheet? Y or N ").upper()
new_gs = "N"

if new_gs == 'Y':
    spreadsheet_Id = create()
    range_name = '表格資料!A1'
    data_range = '表格資料!A2'
elif new_gs == 'N':
    #spreadsheet_link=input("Please paste the spreadsheet link: ")
    spreadsheet_linkss = spreadsheet_links
    spreadsheet_Id = re.split("/", spreadsheet_linkss)[5]
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
        n_cell = int(re.split('(\d+)', cell)[1])+1
        d_cell = re.split('(\d+)', cell)[0] + str(n_cell)
        data_range = sheet_name + '!' + d_cell

        export_pandas_df_to_sheets(spreadsheet_Id, df)

        # print("Error")

        print(df.head())
        # df = df
        #df['data_refresh_time'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for col in df.columns:
            if df[col].dtype == 'datetime64[ns]':
                df = df.astype({col: 'string'})

        for col in df.columns:
            if df[col].dtype == 'datetime64[ns]':
                df = df.astype({col: 'string'})
