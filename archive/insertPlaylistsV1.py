import googleapiclient.discovery, json, os, tkinter

import pandas as pd

from tqdm import tqdm
from tkinter import filedialog
from dotenv import load_dotenv
from urllib.error import HTTPError
from tkinter.messagebox import showinfo
from googleapiclient.discovery import build
# from tkinter.filedialog import askdirectory

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


def chooseCSV():

    filetypes = (
        ('CSV files', '*.csv'),
        ('All files', '*.*')
    )

    filename = filedialog.askopenfilename(
        title='Please select a CSV file',
        filetypes=filetypes)

    showinfo(
        title='Selected File: ',
        message=filename
    )

    return filename

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

    print("Retrieving playlist items and titles... ")
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
            pageToken = PageToken
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
            moreThanOnePlaylist.append(ID)

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

filepath = chooseCSV()

df = pd.read_csv(filepath, header=0)
PLAYLIST_IDS = []
PLAYLIST_NAMES = {}
VIDS_PLAYLISTS = {}

#Retrieve all playlist Ids and playlist names for all channels 
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
sheet_videoIDs = None
if '影片 ID' in df.columns:
    sheet_videoIDs = df['影片 ID']
elif '影片' in df.columns:
    sheet_videoIDs = df['影片']
elif 'Video ID' in df.columns:
    sheet_videoIDs = df['Video ID']
elif 'Video' in df.columns:
    sheet_videoIDs=df['Video']

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
                playlist_name_value = playlist_name_value + str(PLAYLIST_NAMES[i]) + ", "

            playlist_ids.append(playlist_id_value)
            playlist_names.append(playlist_name_value)
        else:
            print(f"Error! Playlist ID for {vidID} is invalid. Playlist ID is invalid type {type(playlist_id)} ")
            playlist_ids.append("")
            playlist_names.append("")

df['欄目'] = playlist_names
df['欄目 ID'] = playlist_ids

df.to_csv(filepath, index = False)