import datetime
from datetime import date
from analytix import Analytics
import requests 
import json
from tqdm import tqdm
from google.auth.transport import Request
from googleapiclient.discovery import build
from urllib.error import HTTPError
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import NVARCHAR, Float, Integer
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
channelID = None

#### commented out by Frank# engine = create_engine('mysql+mysqldb://franklee:frank_2022%402022@184.105.241.89:3306/SOH_YouTube_Analysis')
#### commented out by Frank# con = engine.connect()

dateformat = "%Y-%m-d"
dataStartDate = None
dataEndDate = None

dataStartDate = input('Please enter a starting date for the data. This is REQUIRED 請輸入數據開始日期 YYYY-MM-DD ')
dataStartDate = dataStartDate.strip().upper()
dataEndDate = input('Please enter an ending date for the data. If not, entern "N" 請輸入數據結束日期 YYYY-MM-DD ')
dataEndDate = dataEndDate.strip().upper()

print("如果沒有影片發佈開始日期(影片在此日期後發佈)，可以輸入N")
vidPublishStartDate = input('Videos published after 影片在此日期後發佈: YYYY-MM-DD ')
vidPublishStartDate = vidPublishStartDate.strip().upper()
print("如果沒有影片發佈結束日期(影片在此日期前發佈)，可以輸入N")
vidPublishEndDate = input('Videos published before 影片在此日期前發佈: YYYY-MM-DD ')
vidPublishEndDate = vidPublishEndDate.strip().upper()

# while True:    
#     dataStartDate = input('Please enter a starting date for the data. 請輸入數據開始日期 YYYY-MM-DD ')
#     dataStartDate = dataStartDate.strip()
#     try:
#         datetime.datetime.strptime(dataStartDate, dateformat)
#         print("This is the correct date string format.")
#         break 
#     except ValueError:
#         print("This is the incorrect date string format. It should be YYYY-MM-DD")
#         continue

# while True:    
#     dataEndDate = input('Please enter an ending date for the data. 請輸入數據結束日期 YYYY-MM-DD ')
#     dataEndDate = dataEndDate.strip()
#     # try:
#     #     datetime.datetime.strptime(dataEndDate, dateformat)
#     #     print("This is the correct date string format.")
#     #     break 
#     # except ValueError:
#     #     print("This is the incorrect date string format. It should be YYYY-MM-DD")
#     #     continue



# while True: 
#     print("Please enter the number of the associated channel. 請輸入頻道旁的數字來選擇要獲取數據的頻道")
#     print("1: 希望之聲TV")
#     print("2: 希望之聲粵語頻道")
#     channel_input = input("Please enter the number of the associated channel. 請輸入頻道旁的數字來選擇要獲取數據的頻道")
#     channel_input = channel_input.strip()
#     #希望之聲TV UCk89pEd76qutMB08hVSY49Q
#     #希望之聲粵語頻道 UCCdWF5GML4ai-DVSp0Tgyxg

#     if channel_input.isnumeric() == True:
#         if int(channel_input) == 1: 
#             channelID = 'UCk89pEd76qutMB08hVSY49Q'
#             break
#         elif int(channel_input) == 2: 
#             channelID = 'UCCdWF5GML4ai-DVSp0Tgyxg'
#             break
#         else: 
#             print("Please enter a valid number")
#     else: 
#         print('Please enter a number')

client = Analytics.with_secrets("./YouTubeTEST/SOHclient_secret.json")
# client.authorise(force=True, store_token=False)

youtube = build('youtube', 'v3', developerKey=API_KEY)

METRICS = ['views', 'redViews', 'comments', 'likes', 'dislikes', 'videosAddedToPlaylists', 'videosRemovedFromPlaylists', 'shares', 
                'estimatedMinutesWatched', 'estimatedRedMinutesWatched', 'averageViewDuration', 'averageViewPercentage', 'annotationClickThroughRate', 
                'annotationCloseRate', 'annotationImpressions', 'annotationClickableImpressions', 'annotationClosableImpressions', 'annotationClicks', 
                'annotationCloses', 'cardClickRate', 'cardTeaserClickRate', 'cardImpressions', 'cardTeaserImpressions', 'cardClicks', 'cardTeaserClicks',
                'subscribersGained', 'subscribersLost', 'estimatedRevenue', 'estimatedAdRevenue', 'grossRevenue', 
                'estimatedRedPartnerRevenue', 'monetizedPlaybacks', 'playbackBasedCpm', 'adImpressions', 'cpm']

def getChannelName(channelId):
    request = youtube.channels().list(
        part = 'snippet',
        id=channelId,
        )
    try:
        response = request.execute()
    except HTTPError as e:
        print('Error response status code : {0}, reason : {1}'.format(e.status_code, e.error_details))
    
    return response['items'][0]['snippet']['title']

def getChannelIdAndTitle(vidID):
    request = youtube.videos().list(
        part = 'snippet',
        id=vidID,
        )

    try:
        response = request.execute()
    except HTTPError as e:
        print('Error response status code : {0}, reason : {1}'.format(e.status_code, e.error_details))

    channelName = response['items'][0]['snippet']['channelTitle']
    channelID = response['items'][0]['snippet']['channelId']
    print(channelName, channelID)
    
    return channelName, channelID

def get200VideosData(startDate='N', endDate='N'):
    format = '%Y-%m-%d'
    report = None
    startDate = datetime.datetime.strptime(startDate, format).date()
    print(startDate, endDate)

    if endDate == 'N':
        endDate = date.today()
    else:
        endDate = datetime.datetime.strptime(endDate, format).date()
    report = client.retrieve(start_date=startDate, #data starting date
                end_date=endDate,
                dimensions=['video'], #retrieve data for all videos
                metrics=METRICS, #list of metrics to retrieve data for
                max_results=200, # set maximum number of results (has to be less than 200)
                sort_options=['-views'],  #required parameter. sorts by views in descending order (- means descending)
                include_historical_data=True)

    return report.to_dataframe()

def retrieveAllData(startDate='N', endDate='N', vidPublishBefore='N', vidPublishAfter='N'):
    
    if startDate == 'N':
        return 

    # format = '%Y-%m-%d'
    # startDate = datetime.datetime.strptime(startDate, format).date()
    # if endDate != 'N':
    #     endDate = datetime.datetime.strptime(endDate, format).date()
    # report = None
    # result = None

    # if endDate == 'N':
    #     report = client.retrieve(start_date=startDate, #data starting date
    #                 dimensions=['video'], #retrieve data for all videos
    #                 metrics=METRICS, #list of metrics to retrieve data for
    #                 max_results=200, # set maximum number of results (has to be less than 200)
    #                 sort_options=['-views'],  #required parameter. sorts by views in descending order (- means descending)
    #                 include_historical_data=True)

    #     if vidPublishBefore == 'N':
    #         if vidPublishAfter == 'N': #No data end date or video publish date
    #             pass
    #         else: #No data send date but has video publish after date
    #             pass
    #     else: 
    #         if vidPublishAfter == 'N': #No data end date but has video publish before date
    #             pass
    #         else: #No data end date but has video publish after and video publish end date
    #             pass
    # else:
    #     endDate = datetime.datetime.strptime(endDate, format).date()
    #     report = client.retrieve(start_date=startDate, #data starting date
    #                 end_date=endDate,
    #                 dimensions=['video'], #retrieve data for all videos
    #                 metrics=METRICS, #list of metrics to retrieve data for
    #                 max_results=200, # set maximum number of results (has to be less than 200)
    #                 sort_options=['-views'],  #required parameter. sorts by views in descending order (- means descending)
    #                 include_historical_data=True)

    #     if vidPublishBefore == 'N':
    #         if vidPublishAfter == 'N': #No data end date or video publish date
    #             pass
    #         else:
    #             pass
    #     else: 
    #         if vidPublishAfter == 'N': #No video publish after date but has video publish end date, data end date
    #             pass
    #         else: #Has data end date, video publish before date, video publish after end date 
    #             pass
    
    reportDF = get200VideosData(startDate, endDate)
    
    randomVidID = reportDF['video'][0]
    print(randomVidID)
    channelName, channelID = getChannelIdAndTitle(randomVidID)

    videoIDs = getVidIDs(channelId=channelID, vidPublishAfter=vidPublishAfter, vidPublishEnd=vidPublishBefore)
    videoIDs = set(videoIDs)
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
    reportDF = reportDF.append(dfRestOfVidIds)
    print(reportDF)
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
    url =""
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

def getVidIDs(channelId, VidIDs = [], PageToken = None, vidPublishAfter='N', vidPublishEnd='N'):
    request = None

    if vidPublishAfter == 'N':
        if vidPublishEnd == 'N':
            if PageToken == None:
                request = youtube.search().list(
                channelId = channelId,
                part = "id",
                type='video',
                maxResults = 50
            )
            else:
                request = youtube.search().list(
                    channelId = channelId,
                    part = "id",
                    type='video',
                    maxResults = 50,
                    pageToken=PageToken
                )
        else:
            publishedBefore = str(vidPublishEnd) + "T00:00:00Z"
            if PageToken == None:
                request = youtube.search().list(
                channelId = channelId,
                publishedBefore = publishedBefore,
                part = "id",
                type='video',
                maxResults = 50
            )
            else:
                request = youtube.search().list(
                    channelId = channelId,
                    publishedBefore = publishedBefore,
                    part = "id",
                    type='video',
                    maxResults = 50,
                    pageToken=PageToken
                )
    else:
        publishedAfter = str(vidPublishAfter) + "T00:00:00Z"
        if vidPublishEnd == 'N':
            if PageToken == None:
                request = youtube.search().list(
                channelId = channelId,
                publishedAfter = publishedAfter,
                part = "id",
                type='video',
                maxResults = 50
            )
            else:
                request = youtube.search().list(
                    channelId = channelId,
                    publishedAfter = publishedAfter,
                    part = "id",
                    type='video',
                    maxResults = 50,
                    pageToken=PageToken
                )
        else:
            publishedBefore = str(vidPublishEnd) + "T00:00:00Z"
            if PageToken == None:
                request = youtube.search().list(
                channelId = channelId,
                publishedAfter = publishedAfter,
                publishedBefore = publishedBefore,
                part = "id",
                type='video',
                maxResults = 50
            )
            else:
                publishedBefore = str(vidPublishEnd) + "T00:00:00Z"
                request = youtube.search().list(
                    channelId = channelId,
                    publishedAfter = publishedAfter,
                    publishedBefore = publishedBefore,
                    part = "id",
                    type='video',
                    maxResults = 50,
                    pageToken=PageToken
                )
 

    try:
        response = request.execute()
    except HTTPError as e:
        print('Error response status code : {0}, reason : {1}'.format(e.status_code, e.error_details))

    for i in response['items']:
        VidIDs.append(i['id']['videoId'])

    nextPageToken = response.get('nextPageToken', None)
    if nextPageToken != None:
        nextPageVidIDs = getVidIDs(channelId, VidIDs=VidIDs, PageToken = nextPageToken, vidPublishAfter=vidPublishAfter, vidPublishEnd=vidPublishEnd)
        VidIDs = VidIDs + nextPageVidIDs
        return VidIDs
    else:
        return VidIDs

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
        report = client.retrieve(
            start_date=startDate, #retrieve all data starting from 2021-12-01
            end_date=endDate,
    #  dimensions=['video'], #retrieve data for all videos
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
            result = result.append(df2)


    # report = client.retrieve(
    #         start_date=startDate, #retrieve all data starting from 2021-12-01
    #         end_date=endDate,
    #         dimensions=['video'], #retrieve data for all videos
    #         metrics=METRICS, #list of metrics to retrieve data for
    #         max_results=200, # set maximum number of results (has to be less than 200)
    #         sort_options=['-views'], 
    #         include_historical_data=True) 
    # df = report.to_dataframe()
    
        
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
    df['dataStartDate'] = dataStartDate
    df['dataEndDate'] = dataEndDate

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
     "estimatedRevenue": "你的預估收益 (USD)", "playbackBasedCpm": "CPM (依播放次數) (USD)", "Unique viewers": "非重複觀眾人數"
     , "averageViewDuration": "平均觀看時長" , "averageViewPercentage": "平均觀看比例 (%)", "Average views per viewer": '每位觀眾的平均觀看次數'
     , "dislikes": "不喜歡的人數" , "likes": "喜歡的人數", "PlaylistID": "欄目網址", "Impressions click-through rate": "曝光點閱率 (%)"
     , "shares": "分享次數" , "subscribersLost": "流失的訂閱人數"  , "subscribersGained": "獲得的訂閱人數" , "views": "觀看次數"   , "estimatedMinutesWatched": "觀看時間 (分鐘)" 
     , "CumulativeSubscribers": "訂閱人數" , "RPM": "每千次觀看收益 (USD)" ,  "estimatedHoursWatched": "觀看時間 (小時)", 
    "redViews": "YouTube Premium 觀看次數", "comments": "已新增留言", "videosAddedToPlaylists": "加到播放列表的次數", "videosRemovedFromPlaylists": "從播放列表移除的次數", 
     "estimatedRedMinutesWatched": "YouTube Premium 觀看分鐘", "cardClickRate": "資訊卡點擊率", "cardTeaserClickRate": "資訊卡前導文字點擊率",
     "cardImpressions": "資訊卡曝光次數", "cardTeaserImpressions": "資訊卡前導文字曝光次數", "cardClicks": "資訊卡點擊次數", "cardTeaserClicks": "資訊卡前導文字點擊次數", 
     "estimatedAdRevenue": "YouTube 廣告收益 (USD)", "grossRevenue": "預估總收入", "estimatedRedPartnerRevenue": "您的YouTube Premium收益", "monetizedPlaybacks": "估計營利播放次數",
      "cpm": "CPM 千次曝光出價", "adImpressions": "廣告曝光次數", "videoURL": "影片網址", "video": "影片ID", "channel": "頻道", 'dataStartDate': '數據開始日期', 'dataEndDate': '數據結束日期'}
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

resultsDF, channelName, channelId = retrieveAllData(dataStartDate, dataEndDate, vidPublishEndDate, vidPublishStartDate)

resultsDF = resultsDF.reset_index(drop=False)

AllPID, AllPTitle = getAllPlaylists(channelId)
print(AllPID, AllPTitle)

vidForPlaylists = getVidsForPlaylists(AllPID)

# AllVidIDs = set(getVidIDs(channelID, vidPublishAfter=vidPublishStartDate, vidPublishEnd=vidPublishEndDate))

# channelName = getChannelName(channelID)

# resultsDF = retrieveAnalytics(AllVidIDs, dataStartDate, dataEndDate)    

resultsDF = getVidTitlePublishTime(resultsDF)

resultsDF = addPlaylist(resultsDF, vidForPlaylists, AllPTitle)

resultsDF = getRPM(resultsDF)

resultsDF = HoursWatched(resultsDF)

resultsDF = getCumulativeSubscribersPerVid(resultsDF)

resultsDF= convertNumToMinutes(resultsDF)

resultsDF = insertStartEndDate(resultsDF, dataStartDate, dataEndDate)

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
    # resultsDF.to_json(channelName + dataStartDate + dataEndDate + '.json')
    resultsDF.to_csv(channelName + '.csv')

toSQL = input('Output to SQL SQL? Type Y or N')
toSQL = toSQL.strip().upper()

if toSQL == 'Y':
    print('1 SOHDailyReport')
    print('2 SOHMonthlyReport')
    tableSelected = input('Please enter number corresponding to table: ')

    tableName = None
    if tableSelected == '1':
        tableName = 'SOHDailyReport'
    elif tableSelected == '2':
        tableName = 'SOHMonthlyReport'

    # resultsDF = map_types(resultsDF)
    #### commented out by Frank# resultsDF.to_sql(name=tableName, con=engine, if_exists='append', index=False)

    # print('Please enter your MySQL login information! ')
    # user = input('Username: ')
    # password = input('Password: ')
    # host = input('Hostname: ')
    # port = input('Port: ')
    # database = input('Database: ')
    # engine = create_engine('mysql+mysqldb://' + user + ':' + password + '@' + host + ':' + str(port) + '/' + database)
    # try:
    #     con = engine.connect()
    #     print("Connected!")
    #     resultsDF.to_sql(name=tableName, con=engine, if_exists='append', index=False)
    # except:
    #     print("Connection failed! ")

    # resultsDF.to_sql(name=tableName, con=engine, if_exists='append', index=False)