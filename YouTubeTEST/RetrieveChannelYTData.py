#import necessary modules and packages 
from typing import Dict
import requests 
import json
from tqdm import tqdm
import mysql.connector
from datetime import date
from dotenv import load_dotenv

db = mysql.connector.connect(
    host="localhost",
    user="AppUser",
    passwd="Special888%",
    database="YTTest"
)

class YTstats:
    def __init__(self, api_key, channel_id):
        self.api_key = api_key #Obtain this in your Google Developer Console for YouTube API v3
        self.channel_id = channel_id #Find in channel URL 
        self.channel_statistics = None
        self.video_data = None
    
    def get_channel_statistics(self):
        url = f'https://www.googleapis.com/youtube/v3/channels?part=statistics&id={self.channel_id}&key={self.api_key}'
        print(url)
        json_url = requests.get(url) #Retrieves the content on the page 
        data = json.loads(json_url.text) #Loads data into JSON format 
        print("Data")
        print(data) #data is a JSON object that includes includes kind, etag, pageInfo, and items 
        try: 
            print("Data items")
            print(data["items"]) #items is an array that includes a JSON object that includes: kind, etag, id, and statistics 
            print("0th ele of data items")
            print(data["items"][0]) 
            data=data["items"][0]["statistics"] #Statistics includes viewCount, subscriberCount, hiddenSubscriberCount, and videoCount
            
        except: 
            data = None

        self.channel_statistics = data
        print("Stat data")
        print(data)
        return data

    def dump(self): #dumps channel data into JSON file 
        if self.channel_statistics is None or self.video_data is None: 
            print('Data is none')
            return

        fused_data = {self.channel_id: {"channel_statistics": self.channel_statistics, 
        "video_data": self.video_data}}
        print("fused data")
        print(fused_data)
    
        channel_title = None
        for count, val in enumerate(self.video_data.keys()):
            channel_title = self.video_data[val].get('channelTitle', self.channel_id) #retrieve channel title
            if count > 0 and channel_title is not None:
                break #only run everything inside loop once

        channel_title = channel_title.replace(" ", "_").lower() #replace spaces in channel name and transform to all lowercase 
        file_name = channel_title + ".json" #filename of JSON file
        with open(file_name, 'w') as f: 
            json.dump(fused_data, f, indent=4) #dumps data into JSON file. Indent is an optional parameter to enhance the look of the content 

        print('file dumped')    


    # def SQLUpdate(self):
    #     if self.channel_statistics is None or self.video_data is None: 
    #         print('Data is none')
    #         return

    #     fused_data = {self.channel_id: {"channel_statistics": self.channel_statistics, 
    #     "video_data": self.video_data}}

    #     mycursor = db.cursor() 
    #     #Insert to chartdata
    #     mycursor.execute("INSERT INTO  ")

    def get_channel_videos(self,limit=None): 
        url = f"https://www.googleapis.com/youtube/v3/search?key={self.api_key}&channelId={self.channel_id}&part=id&order=date"
        print("URL:" + url)

        if limit is not None and isinstance(limit, int):
            url += "&maxResults=" + str(limit)

        vid, npt = self._get_channel_videos_per_page(url) #videos and next page Token = 
        counter = 1 # count number of time loop runs
        while (npt is not None and counter <= 10):
            counter += 1 
            nexturl = url + f'&pageToken={npt}' #baseURL + next page token 
            next_vid, npt = self._get_channel_videos_per_page(nexturl)
            vid.update(next_vid)
            pass
        
        return vid

    def _get_channel_videos_per_page(self,url):
        json_url = requests.get(url)
        data = json.loads(json_url.text)
        channel_videos = dict()
        if 'items' not in data:
            return channel_videos, None
        item_data = data['items']
        nextPageToken = data.get("nextPageToken", None)
        for item in item_data:
            try:
                kind = item['id']['kind']
                if kind == 'youtube#video':
                    video_id = item['id']['videoId']
                    channel_videos[video_id] = dict()
                    print(video_id)
            except KeyError:
                print("error")

        return channel_videos, nextPageToken

    def get_channel_video_data(self):
        #1 get all video IDs
        channel_videos = self.get_channel_videos(limit=50)
        print(channel_videos)
        #2 get video statistics
        for i, video_id in enumerate(tqdm(channel_videos)):
            parts = ["snippet","statistics", "contentDetails"] #each part cotaints different types of data
            if i > 500:
                break
            for part in parts: 
                data = self._get_single_video_data(video_id, part)
                channel_videos[video_id].update(data)
                print("Data for " + part)
                print(data)
        print("video data")
        print(channel_videos)
        self.video_data = channel_videos
        return channel_videos

            

    def _get_single_video_data(self, video_id, part):
        url =  f"https://www.googleapis.com/youtube/v3/videos?part={part}&id={video_id}&key={self.api_key}"
        json_url = requests.get(url)
        data = json.loads(json_url.text)
        try: 
            data = data['items'][0][part]
        except:
            print("Error! ")
            data = {}

        return data

#SOH
load_dotenv()
API_KEY = os.getenv("API_KEY")
channel_id = "UCizGWTffp1z_d4oU_gpwl-Q"

yt = YTstats(API_KEY, channel_id)
yt.get_channel_statistics()
yt.get_channel_video_data()
yt.dump()