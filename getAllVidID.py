import os
import json
import requests
import datetime
import pandas as pd
import tkinter as tk
from tqdm import tqdm
from tkinter import ttk
from datetime import date
from dotenv import load_dotenv
from urllib.error import HTTPError
from channel_config import CHANNEL_IDS as CHANNEL_ID_TUPLE
from tkinter.messagebox import showinfo
from googleapiclient.discovery import build
from channel_config import UPLOADS_PLAYLIST_IDS as UPLOADS_PLAYLIST_IDS_TUPLE


load_dotenv()
API_KEY = os.getenv("API_KEY")

youtube = build('youtube', 'v3', developerKey=API_KEY)


def selectChannel(channel=None):
    if channel != None:
        global CHANNEL_SELECTED
        CHANNEL_SELECTED = channel
        return

    # Prompt user to select channel
    root = tk.Tk()

    # config the root window
    root.geometry('500x500')
    root.title('Select Channel')

    label = ttk.Label(text="Please select a channel:", anchor="center")
    label.pack(fill=tk.X, padx=5, pady=5)

    # Available channels for selection
    CHANNEL_NAMES = ['希望之聲粵語頻道', '希望之聲TV', '頭頭是道']
    channel_var = tk.StringVar(root)
    channel_var.set('希望之聲粵語頻道')  # Default selection

    # Create selection box
    w = ttk.Combobox(root, values=CHANNEL_NAMES,
                     textvariable=channel_var, state="readonly")
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

    def on_closing():  # Function to run when window closed
        global CHANNEL_SELECTED
        # save the channel selected to a global variable named CHANNEL_SELECTED
        CHANNEL_SELECTED = channel_var.get()
        root.destroy()  # close the Tkinter root

    root.protocol("WM_DELETE_WINDOW", on_closing)
    root.mainloop()

    return CHANNEL_SELECTED


def getVidIdTimeTitle(channelId, vidPublishAfter, vidPublishEnd="N", VidIDs=[], PageToken=None):

    pass


def getVidIDs(channelId, vidPublishAfter, vidPublishEnd="N", VidIDs=[], PageToken=None):

    request = None
    publishedAfter = str(vidPublishAfter) + "T00:00:00Z"
    if vidPublishEnd != "N":  # If no video publish end date is provided
        publishedBefore = str(vidPublishEnd) + "T00:00:00Z"

        if PageToken == None:
            request = youtube.search().list(
                channelId=channelId,
                publishedAfter=publishedAfter,
                publishedBefore=publishedBefore,
                part="id",
                type='video',
                maxResults=50
            )
        else:
            publishedBefore = str(vidPublishEnd) + "T00:00:00Z"
            request = youtube.search().list(
                channelId=channelId,
                publishedAfter=publishedAfter,
                publishedBefore=publishedBefore,
                part="id",
                type='video',
                maxResults=50,
                pageToken=PageToken
            )
    else:
        if PageToken == None:
            request = youtube.search().list(
                channelId=channelId,
                publishedAfter=publishedAfter,
                part="id",
                type='video',
                maxResults=50
            )
        else:
            publishedBefore = str(vidPublishEnd) + "T00:00:00Z"
            request = youtube.search().list(
                channelId=channelId,
                publishedAfter=publishedAfter,
                part="id",
                type='video',
                maxResults=50,
                pageToken=PageToken
            )

    try:
        response = request.execute()
    except HTTPError as e:
        print('Error response status code : {0}, reason : {1}'.format(
            e.status_code, e.error_details))

    for i in response['items']:
        VidIDs.append(i['id']['videoId'])

    nextPageToken = response.get('nextPageToken', None)
    if nextPageToken != None:
        VidIDs = getVidIDs(channelId, vidPublishAfter=vidPublishAfter,
                           vidPublishEnd=vidPublishEnd, VidIDs=VidIDs, PageToken=nextPageToken)
    return VidIDs


def updateAllVidsIDs(channel_selected):

    CHANNEL_IDS = CHANNEL_ID_TUPLE[0]
    channel_selected_id = CHANNEL_IDS.get(channel_selected)

    df = pd.read_csv(f"./StaticData/{channel_selected}-StaticData.csv")

    # The last time the data was updated
    lastAccessTimestamp = max(df['timestamp'])
    timestamp = datetime.datetime.now().timestamp()  # Today's timestamp

    if lastAccessTimestamp == timestamp:
        return df

    vidPublishAfter = datetime.datetime.fromtimestamp(
        lastAccessTimestamp).strftime("%Y-%m-%d")  # Convert timestamp to string

    videoIDsList = getVidIDs(
        channelId=channel_selected_id, vidPublishAfter=vidPublishAfter)
    videoIDsList = list(set(videoIDsList))  # Remove duplicates

    df2 = pd.DataFrame()
    df2["VideoID"] = list(set(videoIDsList))
    df2['timestamp'] = timestamp
    # df2["VideoID"] = list(set(df["VideoID"]))
    # df = pd.concat([df, df2])

    # df3 = pd.DataFrame()
    # df3["VideoID"] = list(set(df["VideoID"]))
    # df3["timestamp"] = timestamp

    # return df3, videoIDsList
    return df2, videoIDsList


def getAllVidsIDs(channel_selected_id=None):
    print(channel_selected_id)
    CHANNEL_IDS = CHANNEL_ID_TUPLE[0]

    if channel_selected_id != None:
        CHANNEL_SELECTED_ID = channel_selected_id
        CHANNEL_SELECTED = [
            k for k, v in CHANNEL_IDS.items() if v == CHANNEL_SELECTED_ID]
        CHANNEL_SELECTED = CHANNEL_SELECTED[0]
    else:
        CHANNEL_SELECTED = selectChannel()
        CHANNEL_SELECTED_ID = CHANNEL_IDS.get(CHANNEL_SELECTED)

    UPLOADS_PLAYLIST_IDS = UPLOADS_PLAYLIST_IDS_TUPLE[0]
    UPLOADS_PLAYLIST_SELECTED = UPLOADS_PLAYLIST_IDS.get(CHANNEL_SELECTED)

    if UPLOADS_PLAYLIST_SELECTED == None:
        print("Error! Channel uploads playlist not found")

    # If the csv file already exists, we just update it
    if os.path.exists(f"./StaticData/{CHANNEL_SELECTED}-StaticData.csv"):
        df, videos = updateAllVidsIDs(CHANNEL_SELECTED)

    else:
        def is_video(item): return \
            item['snippet']['resourceId']['kind'] == 'youtube#video'

        def video_id(item): return \
            item['snippet']['resourceId']['videoId']

        request = youtube.playlistItems().list(
            playlistId=UPLOADS_PLAYLIST_SELECTED,
            part='snippet',
            fields='nextPageToken,items(snippet(resourceId))',
            maxResults=50
        )
        videos = []

        while request:
            response = request.execute()

            items = response.get('items', [])

            videos.extend(map(video_id, filter(is_video, items)))

            request = youtube.playlistItems().list_next(
                request, response)

        videos = list(set(videos))  # Remove duplicates

        timestamp = datetime.datetime.now().timestamp()

        df = pd.DataFrame()
        df['VideoID'] = videos
        df['timestamp'] = timestamp

        df.to_csv(f"./StaticData/{CHANNEL_SELECTED}-allVidIDs.csv")

    return df


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


def getVidsForPlaylists(playlistIds, excludedPlaylists=[]):

    vidPlaylists = {}

    for i, pId in enumerate(tqdm(playlistIds)):
        if pId in excludedPlaylists:
            continue
        else:
            vidPlaylists.update(getVidsForSinglePlaylist(pId, vidPlaylists))

    print(len(vidPlaylists))
    return vidPlaylists


def getVidsForSinglePlaylist(playlistId, vidPlaylists={}, PageToken=None, APIkey=API_KEY):
    url = ""
    if PageToken == None:
        url = f"https://www.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults=50&playlistId={playlistId}&key={APIkey}"
    else:
        url = f"https://www.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults=50&playlistId={playlistId}&key={APIkey}&pageToken={PageToken}"

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


def getVidTitlePublishTime(videoIDs):
    titles = []
    publishtime = []

    for i in videoIDs:
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

    return titles, publishtime


def generateCSV(channel_selected_id=None):
    CHANNEL_IDS = CHANNEL_ID_TUPLE[0]

    if channel_selected_id != None:
        global CHANNEL_SELECTED_ID
        CHANNEL_SELECTED_ID = channel_selected_id
        CHANNEL_SELECTED = [
            k for k, v in CHANNEL_IDS.items() if v == CHANNEL_SELECTED_ID]
        CHANNEL_SELECTED = CHANNEL_SELECTED[0]
    else:
        CHANNEL_SELECTED = selectChannel()
        CHANNEL_SELECTED_ID = CHANNEL_IDS.get(CHANNEL_SELECTED)

    if os.path.exists(f"./StaticData/{CHANNEL_SELECTED}-StaticData.csv"):
        df0 = pd.read_csv(f"./StaticData/{CHANNEL_SELECTED}-StaticData.csv")

    df, vidIDs = getAllVidsIDs(CHANNEL_SELECTED_ID)
    vidIDs = df['VideoID']
    titles, publishTime = getVidTitlePublishTime(vidIDs)
    playlistIDs, playlistTitles = getAllPlaylists(CHANNEL_SELECTED_ID)

    if len(vidIDs) > 200:
        pass
    else:
        pass

    # Export to CSV file
    df.to_csv(f"./StaticData/{CHANNEL_SELECTED}-StaticData.csv")
    print(f"Successfully outputted data for {CHANNEL_SELECTED}")

# selectChannel()
# getAllVidsIDs()
