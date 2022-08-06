# %%
# IMPORTS
from __future__ import print_function

import json
import os
import re
import string

from tqdm import tqdm
from pytube import Playlist
from dotenv import load_dotenv
from urllib.error import HTTPError
from googleapiclient.discovery import build

load_dotenv()
API_KEY = os.getenv("API_KEY")

youtube = build('youtube', 'v3', developerKey=API_KEY)

# Get some columns back out


# %%

def get_all_playlists(channelId: str, PageToken=None):
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
        nextPagePlaylistIds, nextPagePlaylistTitles = get_all_playlists(
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


def get_vids_from_playlist(playlist_id: str) -> list:
    playlist_url = f"https://www.youtube.com/playlist?list={playlist_id}"
    playlist = Playlist(playlist_url)
    return playlist.video_urls


def get_playlist_info_for_channel(channel_id: str):

    playlist_ids, playlist_names = get_all_playlists(channel_id)
    vid_playlist_ids = {}
    vid_playlist_names = {}
    for index, p_id in enumerate(playlist_ids):
        p_name = playlist_names[p_id]
        video_urls = get_vids_from_playlist(p_id)
        for url in video_urls:
            vid_id = url.split("=")[1]
            init_pid = vid_playlist_ids.get(vid_id, None)
            init_pname = vid_playlist_names.get(p_name, None)
            if init_pid == None:
                vid_playlist_ids[vid_id] = p_id
                vid_playlist_names[vid_id] = p_name
                continue
            if type(init_pid) is list:
                vid_playlist_ids[vid_id] = init_pid.append(p_id)
                vid_playlist_names[vid_id] = init_pname.append(p_name)
                continue
            vid_playlist_ids[vid_id] = [init_pid, p_id]
            vid_playlist_names[vid_id] = [init_pname, p_name]

    return vid_playlist_ids, vid_playlist_names

# %%
