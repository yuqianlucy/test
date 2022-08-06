import analytix
import calendar
import datetime
import json
import os
import requests 

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

from getAllVidID import getAllVidsIDs

#Configuration 
client = Analytics.with_secrets("./ignore/SOHclient_secret.json")
load_dotenv()
API_KEY = os.getenv("API_KEY")
youtube = build('youtube', 'v3', developerKey=API_KEY)    

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

def selectChannel():

        #Prompt user to select channel 
    root = tk.Tk()

    # config the root window
    root.geometry('500x500')
    root.title('Select Channel')

    label = ttk.Label(text="Please select a channel:", anchor="center")
    label.pack(fill=tk.X, padx=5, pady=5)

    CHANNEL_NAMES = ['希望之聲粵語頻道', '希望之聲TV', '頭頭是道'] #Available channels for selection
    channel_var = tk.StringVar(root)
    channel_var.set('希望之聲粵語頻道') #Default selection 

    #Create selection box 
    w = ttk.Combobox(root, values = CHANNEL_NAMES, textvariable=channel_var, state="readonly")
    w.pack()

    # # bind the selected value changes
    def channel_changed(event):
        """ handle the channel changed event """
        print(channel_var.get())
        showinfo(
            title='Channel Selected',
            message=f'You selected {channel_var.get()}!'
        )

    w.bind('<<ComboboxSelected>>', channel_changed)

    def on_closing():
        global CHANNEL_SELECTED
        CHANNEL_SELECTED = channel_var.get() #save the channel selected to a global variable named CHANNEL_SELECTED
        root.destroy() #close the Tkinter root 

    root.protocol("WM_DELETE_WINDOW", on_closing)
    root.mainloop()

def selectMonth():

    root = tk.Tk()

    # config the root window
    root.geometry('300x200')
    root.resizable(False, False)
    root.title('Combobox Widget')

    # label
    label = ttk.Label(text="Please select a month:", anchor="center")
    label.pack(fill=tk.X, padx=5, pady=5)

    # create a combobox
    selected_month = tk.StringVar()
    month_cb = ttk.Combobox(root, textvariable=selected_month)

    # get first 3 letters of every month name
    month_cb['values'] = [month_name[m][0:3] for m in range(1, 13)]

    # prevent typing a value
    month_cb['state'] = 'readonly'

    # place the widget
    month_cb.pack(fill=tk.X, padx=5, pady=5)


    # bind the selected value changes
    def month_changed(event):
        """ handle the month changed event """
        showinfo(
            title='Result',
            message=f'You selected {selected_month.get()}!'
        )


    month_cb.bind('<<ComboboxSelected>>', month_changed)

    # set the current month
    current_month = datetime.datetime.now().strftime('%b')
    month_cb.set(current_month)

    def on_closing():
        global MONTH_SELECTED
        MONTH_SELECTED = selected_month.get() #save the channel selected to a global variable named MONTH_SELECTED
        root.destroy() #close the Tkinter root 

    root.protocol("WM_DELETE_WINDOW", on_closing)
    root.mainloop()

selectChannel()
selectMonth()
datetime_object = datetime.datetime.strptime(MONTH_SELECTED, "%b")
MONTH_NUMBER = datetime_object.month
YEAR =  datetime.datetime.now().year
START_DATE = f"{str(YEAR)}-{MONTH_NUMBER}-01" 
END_DATE = f"{str(YEAR)}-{MONTH_NUMBER}-{str(calendar.monthrange(YEAR, MONTH_NUMBER)[1])}" 

CHANNEL_SELECTED_ID = CHANNEL_IDS.get(CHANNEL_SELECTED) #get selected channel ID

client.authorise(f"./ignore/Tokens/{CHANNEL_SELECTED}") #This allows us to run the code for every channel depending on its token 

startDate = "2022-04-01"
endDate = "2022-04-30"

format = '%Y-%m-%d'
# startDate = datetime.datetime.strptime(startDate, format).date()
# endDate = datetime.datetime.strptime(endDate, format).date()

def get200VideosData(startDate, endDate='N'):
    format = '%Y-%m-%d'
    report = None
    startDate = datetime.datetime.strptime(startDate, format).date()
    
    if endDate == 'N':
        endDate = date.today()
    else:
        endDate = datetime.datetime.strptime(endDate, format).date()
    print(startDate, endDate)

    report = client.retrieve(start_date=startDate, #data starting date
                end_date=endDate,
                dimensions=['video'], #retrieve data for all videos
                metrics=METRICS, #list of metrics to retrieve data for
                max_results=200, # set maximum number of results (has to be less than 200)
                sort_options=['-views'],  #required parameter. sorts by views in descending order (- means descending)
                include_historical_data=True)

    return report.to_dataframe()

# df = get200VideosData(START_DATE, END_DATE)


def retrieveAllData(channelName, channelID, startDate, endDate='N', vidPublishBefore='N', vidPublishAfter='N'):
    
    reportDF = get200VideosData(startDate, endDate)

    # videoIDs = getVidIDs(channelId=channelID, vidPublishAfter=vidPublishAfter, vidPublishEnd=vidPublishBefore)
    # videoIDs = set(videoIDs)

    getAllVidsIDs(CHANNEL_SELECTED_ID)

    df_videoID = pd.read_csv(f"./StaticData/{CHANNEL_SELECTED}-allVidIDs.csv")
    videoIDs = set(df_videoID["VideoID"])
    dfVideoIDs = set(reportDF['video'])

    reportDF = reportDF.set_index('video')

    if vidPublishBefore == 'N' or vidPublishAfter == 'N': #If there is no video publish date specified
        restOfVidIds = videoIDs - dfVideoIDs #gets the rest of the video IDs that need to have data retrieved 
    else:
        alreadyRetrievedVidIds = videoIDs.intersection(dfVideoIDs) #Gets the videoIDs that already have data AND belong to the publish time interval
        unneededVidIds = list(dfVideoIDs - alreadyRetrievedVidIds) #Gets rows that don't belong to publishing time interval
        reportDF = reportDF.drop(index=unneededVidIds) #Removes rows that have the video Ids from a different publishing time interval
        restOfVidIds = videoIDs - alreadyRetrievedVidIds #rest of video Ids that need to have data retrieved for 

    dfRestOfVidIds = retrieveAnalytics(restOfVidIds, startDate, endDate) 
    reportDF = pd.concat([reportDF, dfRestOfVidIds])
    print(reportDF.head())
    return reportDF, channelName, channelID

def getAllPlaylists(channelId, PageToken=None):
    if PageToken == None:
        request = youtube.playlists().list(
        part = 'id, snippet',
        channelId=channelId,
        maxResults=50
        )
    else:
        request = youtube.playlists().list(
        part = 'id, snippet',
        channelId=channelId,
        maxResults=50,
        pageToken = PageToken
        )
    try:
        response = request.execute()
    except HTTPError as e:
        print('Error response status code : {0}, reason : {1}'.format(e.status_code, e.error_details))

    playlistIds = []
    playlistTitles = {}
    for i in tqdm(response['items']):
        playlistIds.append(i['id'])
        playlistTitles[i['id']] = (i['snippet']['localized']['title'])

    nextPageToken = response.get('nextPageToken', None)
    if nextPageToken is not None:
        print("nextPageToken: " + nextPageToken)
        nextPagePlaylistIds, nextPagePlaylistTitles = getAllPlaylists(channelId, PageToken=nextPageToken)
        playlistIds =  playlistIds + nextPagePlaylistIds
        playlistTitles = playlistTitles.update(nextPagePlaylistTitles)
        return playlistIds, playlistTitles 
    else:
        return playlistIds, playlistTitles

def getVidsForPlaylists(playlistIds, excludedPlaylists = []):
    
    vidPlaylists = {}

    for i, pId in enumerate(tqdm(playlistIds)):
        if pId in excludedPlaylists:
            continue
        else:
            vidPlaylists.update(getVidsForSinglePlaylist(pId,vidPlaylists))

    print(len(vidPlaylists))
    return vidPlaylists

def getVidsForSinglePlaylist(playlistId, vidPlaylists={}, PageToken=None, APIkey=API_KEY):
    url = ""
    if PageToken == None:
        url =  f"https://www.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults=50&playlistId={playlistId}&key={APIkey}"
    else:
        url =  f"https://www.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults=50&playlistId={playlistId}&key={APIkey}&pageToken={PageToken}"

    json_url = requests.get(url)
    data = json.loads(json_url.text)

    for j in data['items']: 
        ID = j['contentDetails']['videoId']
        vidPlaylists[ID] = playlistId 
        
    nextPageToken = data.get('nextPageToken', None)
    if nextPageToken != None:
        return getVidsForSinglePlaylist(playlistId, vidPlaylists=vidPlaylists, PageToken=nextPageToken)
    else:
        return vidPlaylists 

def getPlaylistForVidAlternative(AllPlaylistIDs, AllVidIDs):
    vidPlaylist = {}
    playlistOrder = []
    for i in tqdm(AllVidIDs):
        for j in AllPlaylistIDs:
            if j in i:
                vidPlaylist[i] = j
                playlistOrder.append(j)
            continue
    
    return playlistOrder


def retrieveAnalytics(VideoIDs, startDate, endDate):
    format = '%Y-%m-%d'
    result = None
    startDate = datetime.datetime.strptime(startDate, format).date()

    if startDate == 'N':
        return 'Error! A start date was not entered or was not valid'

    if endDate != "N":
        endDate = datetime.datetime.strptime(endDate, format).date()
    else: 
        endDate = date.today()

    for i,val in enumerate(tqdm(VideoIDs)): 
        try: 
            report = client.retrieve(
                start_date=startDate, #retrieve all data starting from 2021-12-01
                end_date=endDate,
        #  dimensions=['video'], #reztrieve data for all videos
                metrics=METRICS, #list of metrics to retrieve data for
                # max_results=200, # set maximum number of results (has to be less than 200)
                filters={'video':val},
                include_historical_data=True) 

            if i == 0 :
                result = report.to_dataframe()
                result['video'] = val
            else:
                df2 = report.to_dataframe() #converts results to dataframe object
                df2['video'] = val
                result = pd.concat([result, df2])
        except requests.exceptions.Timeout:
            print("Timeout occurred")
        except: 
            print(f" {val} was not included in the report")
        
    result = result.set_index('video')
    return result

def getVidTitlePublishTime(df):
    titles = []
    publishtime = []

    for i in df['video']:
        request = youtube.videos().list(
        part = 'snippet',
        id=i)
        try:
            response = request.execute()
            
            # print(response)
            if response['items'] != []:
                title = response["items"][0]['snippet']['localized']['title']
                Ptime = response["items"][0]['snippet']['publishedAt']

                titles.append(title)
                Ptime = datetime.datetime.strptime(Ptime[:10], "%Y-%m-%d").strftime("%m/%d/%Y")
                publishtime.append(Ptime)
            else:
                titles.append("")
                publishtime.append("")
        except HTTPError as e:
            print('Error response status code : {0}, reason : {1}'.format(e.status_code, e.error_details))
            titles.append("")
            publishtime.append("")
    
    df['titles'] = titles
    df['Video Publish Times'] = publishtime

    return df

def addPlaylist(df, vidPlaylist, PTitles):
    playlistName = []
    playlistIDs = []
    for i in df['video']:
        pId = vidPlaylist.get(i, "")
        pTitle = PTitles.get(pId, "無欄目")
        playlistIDs.append(pId)
        playlistName.append(pTitle)
    df['PlaylistID'] = playlistIDs
    df['PlaylistName'] = playlistName

    return df

def addDataRetrivalDates(df):
    df['dataStartDate'] = START_DATE
    df['dataEndDate'] = END_DATE

    return df

def getCumulativeSubscribersPerVid(df):
    df['CumulativeSubscribers'] = df['subscribersGained'] - df['subscribersLost']
    return df

def getRPM(df):
    df['RPM'] = (df['estimatedRevenue'] /df['views']) * 1000
    return df

def HoursWatched(df):
    df['estimatedHoursWatched'] = df['estimatedMinutesWatched'] / 60
    return df

def convertNumToMinutes(df):
    averageViewDuration = []
    for i in df['averageViewDuration']:
        properFormat = str(datetime.timedelta(seconds=int(i)))
        averageViewDuration.append(properFormat)
    df['averageViewDuration'] = averageViewDuration
    return df

def getVideoURL(df):
    df['videoURL'] = "https://www.youtube.com/watch?v=" + df['video'].astype(str)
    return df

def getPlaylistURL(df):
    df['PlaylistID'] = "https://www.youtube.com/playlist?list=" + df['PlaylistID'].astype(str)
    return df

def insertStartEndDate(df, StartDate, EndDate):
    df['dataStartDate'] = StartDate
    if EndDate == 'N':
        EndDate = date.today()
    df['dataEndDate'] = EndDate
    return df

def insertChannelName(df, channelName):
    df['channel'] = channelName
    return df

def addEmptyEnglishColumns(df):
    df['Impressions click-through rate'] = ""
    df['Average views per viewer'] = ""
    df['Unique viewers'] = ""
    df['Impressions'] = ""

    return df

def reorderColumns(df):
    # reorder = ['channel', 'titles', 'PlaylistName', 'Video Publish Times',
    #  'estimatedRevenue', 'RPM', 'playbackBasedCpm', '曝光點閱率 (%)', '每位觀眾的平均觀看次數', 
    #  '非重複觀眾人數', 'averageViewPercentage', 'averageViewDuration', 'estimatedAdRevenue', 'dislikes', 
    #  'likes', 'shares', 'subscribersLost', 'subscribersGained', 'views', 'estimatedHoursWatched', 
    #  'CumulativeSubscribers', '曝光次數', 'views', 'estimatedRedMinutesWatched', 'estimatedRedPartnerRevenue', 'monetizedPlaybacks',
    #   'grossRevenue', 'cpm', 'estimatedMinutesWatched', 'comments', 'cardImpressions', 'cardTeaserImpressions',
    #    'adImpressions', 'cardClicks', 'cardTeaserClicks', 'cardClickRate', 'cardTeaserClickRate', 
    #    'videosAddedToPlaylists', '從播放列表移除的次數欄目網址', 'video']

    reorder = ['channel', 'titles', 'PlaylistName', 'Video Publish Times',
     'estimatedRevenue', 'RPM', 'playbackBasedCpm', 'Impressions click-through rate', 'Average views per viewer', 'Unique viewers',
     'averageViewPercentage', 'averageViewDuration', 'dislikes', 
     'likes', 'shares', 'subscribersLost', 'subscribersGained', 'views', 'estimatedHoursWatched', 
     'CumulativeSubscribers', 'Impressions', 'adImpressions','comments', 'estimatedAdRevenue',
      'redViews','estimatedRedMinutesWatched', 'estimatedRedPartnerRevenue', 'monetizedPlaybacks',
      'grossRevenue', 'cpm', 'estimatedMinutesWatched', 'cardImpressions', 'cardTeaserImpressions',
       'cardClicks', 'cardTeaserClicks', 'cardClickRate', 'cardTeaserClickRate', 
       'videosAddedToPlaylists', 'videosRemovedFromPlaylists','video','videoURL', 'PlaylistID','dataStartDate', 'dataEndDate']
    return df[reorder]

def getChineseColumns(df):
    translation = {'Video Publish Times': '影片發布時間', "PlaylistName": "欄目", "titles": "影片標題" , "Impressions": "曝光次數",
     "estimatedRevenue": "你的預估收益 (USD)", "playbackBasedCpm": "CPM 以播放次數為準的每千次展示成本", "Unique viewers": "非重複觀眾人數"
     , "averageViewDuration": "平均觀看時長" , "averageViewPercentage": "平均觀看比例 (%)", "Average views per viewer": '每位觀眾的平均觀看次數'
     , "dislikes": "不喜歡次數" , "likes": "喜歡次數", "PlaylistID": "欄目網址", "Impressions click-through rate": "曝光點閱率 (%)"
     , "shares": "分享次數" , "subscribersLost": "流失的訂閱人數"  , "subscribersGained": "獲得的訂閱人數" , "views": "觀看次數"   , "estimatedMinutesWatched": "估計觀看時間 (分鐘)" 
     , "CumulativeSubscribers": "訂閱人數" , "RPM": "每千次觀看收益 (USD)" ,  "estimatedHoursWatched": "估計觀看時間 (小時)", 
    "redViews": "YouTube Premium 觀看次數", "comments": "已新增留言", "videosAddedToPlaylists": "加到播放列表的次數", "videosRemovedFromPlaylists": "從播放列表移除的次數", 
     "estimatedRedMinutesWatched": "YouTube Premium 觀看分鐘", "cardClickRate": "資訊卡點擊率", "cardTeaserClickRate": "資訊卡前導文字點擊率",
     "cardImpressions": "資訊卡曝光次數", "cardTeaserImpressions": "資訊卡前導文字曝光次數", "cardClicks": "資訊卡點擊次數", "cardTeaserClicks": "資訊卡前導文字點擊次數", 
     "estimatedAdRevenue": "預估YouTube 廣告收益 (USD)", "grossRevenue": "DoubleClick 和Google 售出的廣告收益 (USD)", "estimatedRedPartnerRevenue": "您的YouTube Premium收益", "monetizedPlaybacks": "估計營利播放次數",
      "cpm": "CPM  每千次展示成本", "adImpressions": "廣告曝光次數", "videoURL": "影片網址", "video": "影片ID", "channel": "頻道", 'dataStartDate': '數據開始日期', 'dataEndDate': '數據結束日期'}
    return df.rename(columns = translation)

def addEmptyChineseColumns(df):
    df['曝光點閱率 (%)'] = ""
    df['每位觀眾的平均觀看次數'] = ""
    df['非重複觀眾人數'] = ""
    df['曝光次數'] = ""

    return df

def reorderChineseColumns(df):
    return df[['頻道', "影片標題", "欄目", "影片發布時間", "你的預估收益 (USD)",
     "每千次觀看收益 (USD)", "CPM (依播放次數) (USD)", "曝光點閱率 (%)", "每位觀眾的平均觀看次數", "非重複觀眾人數", 
     "平均觀看比例 (%)", "平均觀看時長", "不喜歡的人數", "喜歡的人數", "分享次數", "流失的訂閱人數", 
     "獲得的訂閱人數", "觀看次數", "觀看時間 (小時)", "訂閱人數", "曝光次數","廣告曝光次數", "已新增留言","YouTube 廣告收益 (USD)",
       "YouTube Premium 觀看次數",  "YouTube Premium 觀看分鐘", "您的YouTube Premium收益", "估計營利播放次數", "預估總收入", 
      "CPM 千次曝光出價", "觀看時間 (分鐘)", "資訊卡曝光次數", "資訊卡前導文字曝光次數",  
      "資訊卡點擊次數", "資訊卡前導文字點擊次數", "資訊卡點擊率", "資訊卡前導文字點擊率", "加到播放列表的次數", "從播放列表移除的次數"
        "影片ID", "影片網址", "欄目網址", '數據開始日期', '數據結束日期']]
    
def map_types(df):
    dtypedict = {}
    for i, j in zip(df.columns, df.dtypes):
        if "object" in str(j):
            dtypedict.update({i: NVARCHAR(length=255)})
        if "float" in str(j):
            dtypedict.update({i: Float(precision=2, asdecimal=True)})
        if "int" in str(j):
            dtypedict.update({i: Integer()})
    return dtypedict

def chooseSaveDirectory(locationTxt):

    if os.path.isfile(locationTxt):
        file = open(locationTxt, "r+")
        storeLocation = file.readline()
        file.close()

        if storeLocation.is_dir():
            filepath = askdirectory(
                title="Please choose where you would like to save the file",
                initialdir=storeLocation
                )
        else:
            filepath = askdirectory(
                title="Please choose where you would like to save the file"
                )
    else:
        filepath = askdirectory(
            title="Please choose where you would like to save the file"
            )

    return filepath
    

#write to local db
# def create_server_connection(host_name, user_name, user_password):
#     connection = None
#     try:
#         connection = mysql.connector.connect(
#             host=host_name,
#             user=user_name,
#             passwd=user_password
#         )
#         print("MySQL Database connection successful")
#     except Error as err:
#         print(f"Error: '{err}'")

#     return connection

# connection = create_server_connection("localhost", "root", "password")

resultsDF, channelName, channelId = retrieveAllData(CHANNEL_SELECTED, CHANNEL_SELECTED_ID, START_DATE, END_DATE)

resultsDF = resultsDF.reset_index(drop=False)

AllPID, AllPTitle = getAllPlaylists(channelId)
print(AllPID, AllPTitle)

vidForPlaylists = getVidsForPlaylists(AllPID)   

resultsDF = getVidTitlePublishTime(resultsDF)

resultsDF = addPlaylist(resultsDF, vidForPlaylists, AllPTitle)

resultsDF = getRPM(resultsDF)

resultsDF = HoursWatched(resultsDF)

resultsDF = getCumulativeSubscribersPerVid(resultsDF)

resultsDF= convertNumToMinutes(resultsDF)

resultsDF = insertStartEndDate(resultsDF, START_DATE, END_DATE)

resultsDF = insertChannelName(resultsDF, channelName)

resultsDF = getVideoURL(resultsDF)

resultsDF = getPlaylistURL(resultsDF)

resultsDF = addEmptyEnglishColumns(resultsDF)

resultsDF = reorderColumns(resultsDF)

resultsDF = getChineseColumns(resultsDF)

print(resultsDF)

toCSV = input('Create CSV? Type Y or N')
toCSV = toCSV.strip().upper()

if toCSV == 'Y':

    locationTxt = './CSVfilepath.txt'
    filepath = chooseSaveDirectory(locationTxt)

    resultsDF.to_csv(filepath+ "/" + channelName + '.csv')

    with open(locationTxt, 'w') as f:
        f.write(filepath)

# toJSON = input('Create CSV? Type Y or N')
# toJSON = toJSON.strip().upper()

# if toJSON == 'Y':
#     resultsDF.to_json(channelName + START_DATE + END_DATE + '.json')

# toSQL = input('Output to SQL SQL? Type Y or N')
# toSQL = toSQL.strip().upper()

# if toSQL == 'Y':
#     print('1 SOHDailyReport')
#     print('2 SOHMonthlyReport')
#     tableSelected = input('Please enter number corresponding to table: ')

#     tableName = None
#     if tableSelected == '1':
#         tableName = 'SOHDailyReport'
#     elif tableSelected == '2':
#         tableName = 'SOHMonthlyReport'