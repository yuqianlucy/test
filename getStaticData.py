# Static data includes video ID, publish time, title, playlist id, playlist name
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
from tkinter.messagebox import showinfo
from googleapiclient.discovery import build

load_dotenv()
API_KEY = os.getenv("API_KEY")

youtube = build('youtube', 'v3', developerKey=API_KEY)

CHANNEL_IDS = {
    "希望之聲粵語頻道": "UCCdWF5GML4ai-DVSp0Tgyxg",
    "希望之聲TV": "UCk89pEd76qutMB08hVSY49Q",
    "頭頭是道": "UCizGWTffp1z_d4oU_gpwl-Q"
}

UPLOADS_PLAYLIST_IDS = {
    "希望之聲粵語頻道": "UUCdWF5GML4ai-DVSp0Tgyxg",
    "希望之聲TV": "UUk89pEd76qutMB08hVSY49Q",
    "頭頭是道": "UUizGWTffp1z_d4oU_gpwl-Q"
}


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


def getChannelId(CHANNEL_SELECTED):
    CHANNEL_IDS = {
        "希望之聲粵語頻道": "UCCdWF5GML4ai-DVSp0Tgyxg",
        "希望之聲TV": "UCk89pEd76qutMB08hVSY49Q",
        "頭頭是道": "UCizGWTffp1z_d4oU_gpwl-Q"
    }
    return CHANNEL_IDS.get(CHANNEL_SELECTED)


def filterStaticData(item, publish_after=None):
    kind = item['snippet']['resourceId']['kind']
    privacyStatus = item['status']['privacyStatus']

    if publish_after == None:

        # Checks if publish_after is datetime object
        if isinstance(publish_after, datetime.datetime):
            return f"Error! parameter publish_after should be datetime object instead of {type(publish_after)}"

        videoPublishedAt = item['contentDetails']['videoPublishedAt']
        videoPublishedAt = datetime.datetime.strptime(
            videoPublishedAt, "%Y-%m-%dT%H:%M:%SZ")
        return (videoPublishedAt >= publish_after and kind == 'youtube#video' and privacyStatus == "public")

    return (kind == 'youtube#video' and privacyStatus == "public")


def retrieveStaticData(CHANNEL_SELECTED_ID, PUBLISH_AFTER=None):

    UPLOADS_PLAYLIST_IDS = {
        "UCCdWF5GML4ai-DVSp0Tgyxg": "UUCdWF5GML4ai-DVSp0Tgyxg",
        "UCk89pEd76qutMB08hVSY49Q": "UUk89pEd76qutMB08hVSY49Q",
        "UCizGWTffp1z_d4oU_gpwl-Q": "UUizGWTffp1z_d4oU_gpwl-Q"
    }
    uploads_id = UPLOADS_PLAYLIST_IDS.get(CHANNEL_SELECTED_ID)

    request = youtube.playlistItems().list(
        playlistId=uploads_id,
        part='snippet',
        fields='nextPageToken,items(snippet(resourceId)), items(contentDetails(videoPublishedAt))',
        maxResults=50
    )

    request = youtube.playlistItems().list(
        playlistId="UUk89pEd76qutMB08hVSY49Q",
        part='snippet',
        fields='nextPageToken,items',
        maxResults=50
    )

    videos = []
    
    while request:
        response = request.execute()

        items = response.get('items', [])

        filteredData = filter(lambda seq: filterStaticData(seq, 3), items)

        videos.extend(map(video_id, filteredData))

        request = youtube.playlistItems().list_next(
            request, response)

    if PUBLISH_AFTER == None:
        pass
    else:
        pass
    pass

############################
    

    
#################################

def toCSV(CHANNEL_SELECTED_ID, CHANNEL_SELECTED, df):

    CHANNEL_IDS = {
        "希望之聲粵語頻道": "UCCdWF5GML4ai-DVSp0Tgyxg",
        "希望之聲TV": "UCk89pEd76qutMB08hVSY49Q",
        "頭頭是道": "UCizGWTffp1z_d4oU_gpwl-Q"
    }

    if CHANNEL_SELECTED == None:
        if CHANNEL_SELECTED_ID == None:
            CHANNEL_SELECTED = selectChannel()
        CHANNEL_SELECTED_ID = CHANNEL_IDS.get(CHANNEL_SELECTED)
    elif CHANNEL_SELECTED_ID == None:
        CHANNEL_SELECTED = [
            k for k, v in CHANNEL_IDS.items() if v == CHANNEL_SELECTED_ID]
        CHANNEL_SELECTED = CHANNEL_SELECTED[0]

    if os.path.exists(f"./StaticData/{CHANNEL_SELECTED}-StaticData.csv"):
        df0 = pd.read_csv(f"./StaticData/{CHANNEL_SELECTED}-StaticData.csv")

        # The last time the data was updated
        lastAccessTimestamp = max(df0['timestamp'])

        vidPublishAfter = datetime.datetime.fromtimestamp(
            lastAccessTimestamp).strftime("%Y-%m-%d")  # Convert timestamp to string

    else:
        pass


    timestamp = datetime.datetime.now().timestamp()  # Today's timestamp

    # df, vidIDs = getAllVidsIDs(CHANNEL_SELECTED_ID)
    # vidIDs = df['VideoID']
    # titles, publishTime = getVidTitlePublishTime(vidIDs)
    # playlistIDs, playlistTitles = getAllPlaylists(CHANNEL_SELECTED_ID)

    # if len(vidIDs) > 200:
    #     pass
    # else:
    #     pass
