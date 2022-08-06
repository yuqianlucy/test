import datetime, schedule, time, json, requests
from datetime import date, timedelta
from re import L
from analytix import Analytics
from tqdm import tqdm
from google.auth.transport import Request
from googleapiclient.discovery import build
from urllib.error import HTTPError
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import NVARCHAR, Float, Integer
from dotenv import load_dotenv
import os

load_dotenv()
API_KEY = os.getenv("API_KEY")
channelID = 'UCk89pEd76qutMB08hVSY49Q'

engine = create_engine('mysql+mysqldb://franklee:frank_2022%402022@184.105.241.89:3306/SOH_YouTube_Analysis')
con = engine.connect()

def func():

    client = Analytics.with_secrets("../ignore/SOHclient_secret.json")

    youtube = build('youtube', 'v3', developerKey=API_KEY)

    def getYesterdayDate():
        today = date.today()
        yesterday = today - timedelta(days=1)
        return yesterday
    
    def getDayBeforeYesterday():
        today = date.today()
        dayBeforeYesterday = today - timedelta(days=2)
        return dayBeforeYesterday

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
        print('fetching playlists')
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

        print('Fetching videos for playlists')
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

    def getVidIDs(channelId, VidIDs = [], PageToken = None):
        request = None

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

        try:
            response = request.execute()
        except HTTPError as e:
            print('Error response status code : {0}, reason : {1}'.format(e.status_code, e.error_details))

        print('fetching all videos')
        for i in tqdm(response['items']):
            VidIDs.append(i['id']['videoId'])

        nextPageToken = response.get('nextPageToken', None)
        if nextPageToken != None:
            nextPageVidIDs = getVidIDs(channelId, VidIDs=VidIDs, PageToken = nextPageToken)
            VidIDs = VidIDs + nextPageVidIDs
            return VidIDs
        else:
            return VidIDs

    def retrieveAnalytics(VideoIDs, startDate, endDate):
        format = '%Y-%m-%d'
        result = None
        
        # startDate = datetime.datetime.strptime(startDate, format).date()
        # endDate = datetime.datetime.strptime(endDate, format).date()
        for i,val in enumerate(tqdm(VideoIDs)): 
            report = client.retrieve(
                start_date=startDate, #retrieve all data starting from 2021-12-01
                end_date=endDate,
        #  dimensions=['video'], #retrieve data for all videos
                metrics=['views', 'redViews', 'comments', 'likes', 'dislikes', 'videosAddedToPlaylists', 'videosRemovedFromPlaylists', 'shares',
                'estimatedMinutesWatched', 'estimatedRedMinutesWatched', 'averageViewDuration', 'averageViewPercentage', 
                'cardClickRate', 'cardTeaserClickRate', 'cardImpressions', 'cardTeaserImpressions', 'cardClicks', 'cardTeaserClicks',
                    'subscribersGained', 'subscribersLost', 'estimatedRevenue', 'estimatedAdRevenue', 'grossRevenue', 'estimatedRedPartnerRevenue', 
                    'monetizedPlaybacks', 'playbackBasedCpm', 'adImpressions', 'cpm'], #list of metrics to retrieve data for
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
    #         metrics=['views', 'redViews', 'comments', 'likes', 'dislikes', 'videosAddedToPlaylists', 'videosRemovedFromPlaylists', 'shares',
    #         'estimatedMinutesWatched', 'estimatedRedMinutesWatched', 'averageViewDuration', 'averageViewPercentage', 
    #         'cardClickRate', 'cardTeaserClickRate', 'cardImpressions', 'cardTeaserImpressions', 'cardClicks', 'cardTeaserClicks',
    #             'subscribersGained', 'subscribersLost', 'estimatedRevenue', 'estimatedAdRevenue', 'grossRevenue', 'estimatedRedPartnerRevenue', 
    #             'monetizedPlaybacks', 'playbackBasedCpm', 'adImpressions', 'cpm'], #list of metrics to retrieve data for
    #         max_results=200, # set maximum number of results (has to be less than 200)
    #         sort_by=['-views'], 
    #         include_historical_data=True) 
    # df = report.to_dataframe()
    
            
        result = result.reset_index(drop=True)
        return result

    def getVidTitlePublishTime(df):
        titles = []
        publishtime = []

        for i in tqdm(df['video']):
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
        print('Fetching playlists')
        for i in tqdm(df['video']):
            pId = vidPlaylist.get(i, "")
            pTitle = PTitles.get(pId, "無欄目")
            playlistIDs.append(pId)
            playlistName.append(pTitle)
        df['PlaylistID'] = playlistIDs
        df['PlaylistName'] = playlistName

        return df.reset_index(drop=True)

    def addDataRetrivalDates(df):
        df['dataStartDate'] = dataStartDate
        df['dataEndDate'] = dataEndDate

        return df

    def getCumulativeSubscribersPerVid(df):
        df['CumulativeSubscribers'] = df['subscribersGained'] - df['subscribersLost']
        return df.reset_index(drop=True)

    def getRPM(df):
        df['RPM'] = (df['estimatedRevenue'] /df['views']) * 1000
        return df.reset_index(drop=True)

    def HoursWatched(df):
        df['estimatedHoursWatched'] = df['estimatedMinutesWatched'] / 60
        return df

    def convertNumToMinutes(df):
        averageViewDuration = []
        print('fetching average view duration')
        for i in tqdm(df['averageViewDuration']):
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


    yesterday = getYesterdayDate()
    dayBeforeYesterday = getDayBeforeYesterday()

    AllPID, AllPTitle = getAllPlaylists(channelID)
    print(AllPID, AllPTitle)

    vidForPlaylists = getVidsForPlaylists(AllPID)

    AllVidIDs = set(getVidIDs(channelID))

    channelName = getChannelName(channelID)

    resultsDF = retrieveAnalytics(AllVidIDs, dayBeforeYesterday, yesterday)    

    resultsDF = getVidTitlePublishTime(resultsDF)

    resultsDF = addPlaylist(resultsDF, vidForPlaylists, AllPTitle)

    resultsDF = getRPM(resultsDF)

    resultsDF = HoursWatched(resultsDF)

    resultsDF = getCumulativeSubscribersPerVid(resultsDF)

    resultsDF= convertNumToMinutes(resultsDF)

    resultsDF = insertStartEndDate(resultsDF, dayBeforeYesterday, yesterday)

    resultsDF = insertChannelName(resultsDF, channelName)

    resultsDF = getVideoURL(resultsDF)

    resultsDF = getPlaylistURL(resultsDF)

    resultsDF = addEmptyEnglishColumns(resultsDF)

    resultsDF = reorderColumns(resultsDF)

    resultsDF = getChineseColumns(resultsDF)

    # resultsDF.to_json(channelName + dataStartDate + dataEndDate + '.json')
    resultsDF.to_csv(channelName + '.csv')

    # # resultsDF = map_types(resultsDF)
    resultsDF.to_sql(name='''SOHDailyReport''', con=engine, if_exists='append', index=False)

schedule.every().day.at("11:55").do(func)

while True:
    schedule.run_pending()
    time.sleep(1)