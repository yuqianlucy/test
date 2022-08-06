# Static data includes video ID, publish time, title, playlist id, playlist name
import os
import math
import json
from numpy import i0
import requests
import datetime
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv
from urllib.error import HTTPError
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

# #希望之聲TV
# playlistIds = [ 'PLy4vsOfbMmQBzL2QSSQHPk2yMIL9QrtYT', 'PLy4vsOfbMmQBE5CGs-wUoeyP9XFMrCwxk',
#                'PLy4vsOfbMmQA0emvlfA8KhNoVWGY0D6DA', 'PLy4vsOfbMmQBWOK8zZ93E9ODo2Dw1ABv_', 'PLy4vsOfbMmQD_0Rg4qUIHGQFO9gVjnBPd',
#                'PLy4vsOfbMmQAuFTmkhKSy4PHHyEsu6X__', 'PLy4vsOfbMmQB4TsswGdU6l1ie04nkfM9u', 'PLy4vsOfbMmQCvjXySwbuldIJSNs8zI549',
#                'PLy4vsOfbMmQATTzUebXKw_gapfxb4IAbY', 'PLy4vsOfbMmQCOd_FZU1-ApAeRnEOeR8de', "PLy4vsOfbMmQCveb4qKZNKqMAgpdrIIhHt"
#                "PLy4vsOfbMmQBJ3OECiZVryCbmFDMwbgaJ", "PLy4vsOfbMmQDClwNJ8luDSUSrzDNmxyu6", "PLy4vsOfbMmQAwjT25VZQOxjj5vDGG_OEC"]

# playlistNames = {"PLy4vsOfbMmQCOd_FZU1-ApAeRnEOeR8de": "法轮功真相",
#                  "PLy4vsOfbMmQA0emvlfA8KhNoVWGY0D6DA": "精彩回放",
#                  "PLy4vsOfbMmQBE5CGs-wUoeyP9XFMrCwxk": "熱點直播 專家解讀",
#                  "PLy4vsOfbMmQCveb4qKZNKqMAgpdrIIhHt": "新聞觀察",
#                  "PLy4vsOfbMmQBJ3OECiZVryCbmFDMwbgaJ": "時事關心",
#                  "PLy4vsOfbMmQDClwNJ8luDSUSrzDNmxyu6": "國際風雲",
#                  "PLy4vsOfbMmQAwjT25VZQOxjj5vDGG_OEC": "北美新聞",
#                  "PLy4vsOfbMmQATTzUebXKw_gapfxb4IAbY": "每日頭條",
#                  "PLy4vsOfbMmQCvjXySwbuldIJSNs8zI549": "財經慧眼",
#                  "PLy4vsOfbMmQB4TsswGdU6l1ie04nkfM9u": "環球看點",
#                  "PLy4vsOfbMmQAuFTmkhKSy4PHHyEsu6X__": "兩岸要聞",
#                  "PLy4vsOfbMmQD_0Rg4qUIHGQFO9gVjnBPd": "辛恬面對面",
#                  "PLy4vsOfbMmQBWOK8zZ93E9ODo2Dw1ABv_": "紅朝禁聞",
#                  "PLy4vsOfbMmQBzL2QSSQHPk2yMIL9QrtYT": "美國觀察大選倒計時"
#                  }

# # 希望之聲粵語頻道
# playlistIds = [
#     "PLzrFT7CrcDKzurwBd3kKW2vSkH1g8PfsC", "PLzrFT7CrcDKzqJjFo5a4OVmUbW-Iv_g4W", "PLzrFT7CrcDKxk0YC8p0gmofZ69SBr535F",
#     "PLzrFT7CrcDKwfum4mMLIj_ss7yej2kLhY", "PLzrFT7CrcDKwcBIKHWtSQ3Hnz-xA-1Beg", "PLzrFT7CrcDKzTLFnmxNY3cshgppV6IRj1",
#     "PLzrFT7CrcDKxGYEHrnqExnbgS6XN3T38j", "PLzrFT7CrcDKxBmIk6daYdruBNVen1QY8x", "PLzrFT7CrcDKzmAx8_b-PJVt5AsChQSo4T",
#     "PLzrFT7CrcDKw6fwKZ8MycOJmfvUXHg80a", "PLzrFT7CrcDKyl4kzjmta-luoLfaWLIUXL", "PLzrFT7CrcDKzQWthZAtsdUmCXRYj0VKI6",
#     "PLzrFT7CrcDKxbW_UYHWrLKmSiTk26iI6G", "PLzrFT7CrcDKwosbObrScXXRh2AQMfd7bJ", "PLzrFT7CrcDKwWhrP_oUztwvVpkfly82YN",
#     "PLzrFT7CrcDKxGGVal3sZIT1tNxt5khXaO", "PLzrFT7CrcDKxlkYPylp0GGzmeu824M9F6",
#      'PLy4vsOfbMmQBzL2QSSQHPk2yMIL9QrtYT', 'PLy4vsOfbMmQBE5CGs-wUoeyP9XFMrCwxk',
# ]
# playlistNames = {
#     "PLzrFT7CrcDKzurwBd3kKW2vSkH1g8PfsC": "香港疫情追踪",
#     "PLzrFT7CrcDKzqJjFo5a4OVmUbW-Iv_g4W": "新聞熱點",
#     "PLzrFT7CrcDKxk0YC8p0gmofZ69SBr535F": "財經熱點",
#     "PLzrFT7CrcDKwfum4mMLIj_ss7yej2kLhY": "週末故事",
#     "PLzrFT7CrcDKwcBIKHWtSQ3Hnz-xA-1Beg": "財經觀察站",
#     "PLzrFT7CrcDKzTLFnmxNY3cshgppV6IRj1": "路橋點評",
#     "PLzrFT7CrcDKxGYEHrnqExnbgS6XN3T38j": "香港簡訊",
#     "PLzrFT7CrcDKxBmIk6daYdruBNVen1QY8x": "「希望聽新聞」（粵語）",
#     "PLzrFT7CrcDKzmAx8_b-PJVt5AsChQSo4T": "頭頭是道",
#     "PLzrFT7CrcDKw6fwKZ8MycOJmfvUXHg80a": "時事解碼",
#     "PLzrFT7CrcDKyl4kzjmta-luoLfaWLIUXL": "環球要聞",
#     "PLzrFT7CrcDKzQWthZAtsdUmCXRYj0VKI6": "焦點新聞",
#     "PLzrFT7CrcDKxbW_UYHWrLKmSiTk26iI6G": "每日要聞",
#     "PLzrFT7CrcDKwosbObrScXXRh2AQMfd7bJ": "中國經濟熱點",
#     "PLzrFT7CrcDKwWhrP_oUztwvVpkfly82YN": "粵覽新聞",
#     "PLzrFT7CrcDKxGGVal3sZIT1tNxt5khXaO": "粵講粵有理",
#     "PLzrFT7CrcDKxlkYPylp0GGzmeu824M9F6": "時事熱評"
# }

# 希望之聲中文TV
playlistIds = [
    "PLzrFT7CrcDKzurwBd3kKW2vSkH1g8PfsC", "PLzrFT7CrcDKzqJjFo5a4OVmUbW-Iv_g4W", "PLzrFT7CrcDKxk0YC8p0gmofZ69SBr535F",
    "PLzrFT7CrcDKwfum4mMLIj_ss7yej2kLhY", "PLzrFT7CrcDKwcBIKHWtSQ3Hnz-xA-1Beg", "PLzrFT7CrcDKzTLFnmxNY3cshgppV6IRj1",
    "PLzrFT7CrcDKxGYEHrnqExnbgS6XN3T38j",  "PLzrFT7CrcDKzmAx8_b-PJVt5AsChQSo4T", "PLy4vsOfbMmQDoruUdSQCspRH_sjhk6t9R",
    "PLzrFT7CrcDKxBmIk6daYdruBNVen1QY8x",
    "PLzrFT7CrcDKw6fwKZ8MycOJmfvUXHg80a", "PLzrFT7CrcDKyl4kzjmta-luoLfaWLIUXL", "PLzrFT7CrcDKzQWthZAtsdUmCXRYj0VKI6",
    "PLzrFT7CrcDKxbW_UYHWrLKmSiTk26iI6G", "PLzrFT7CrcDKwosbObrScXXRh2AQMfd7bJ", "PLzrFT7CrcDKwWhrP_oUztwvVpkfly82YN",
    "PLzrFT7CrcDKxGGVal3sZIT1tNxt5khXaO", "PLzrFT7CrcDKxlkYPylp0GGzmeu824M9F6", "PLzrFT7CrcDKyyG1Q6jdu-bKlOQ_7o5SJ2",
    'PLy4vsOfbMmQA0emvlfA8KhNoVWGY0D6DA', 'PLy4vsOfbMmQBWOK8zZ93E9ODo2Dw1ABv_', 'PLy4vsOfbMmQD_0Rg4qUIHGQFO9gVjnBPd',
    'PLy4vsOfbMmQAuFTmkhKSy4PHHyEsu6X__', 'PLy4vsOfbMmQB4TsswGdU6l1ie04nkfM9u', 'PLy4vsOfbMmQCvjXySwbuldIJSNs8zI549',
    'PLy4vsOfbMmQATTzUebXKw_gapfxb4IAbY', 'PLy4vsOfbMmQCOd_FZU1-ApAeRnEOeR8de', "PLy4vsOfbMmQCveb4qKZNKqMAgpdrIIhHt",
    "PLy4vsOfbMmQBJ3OECiZVryCbmFDMwbgaJ", "PLy4vsOfbMmQDClwNJ8luDSUSrzDNmxyu6", "PLy4vsOfbMmQAwjT25VZQOxjj5vDGG_OEC",
    "PLy4vsOfbMmQC15IH7EY3ysN41rruZffQ0"
]
playlistNames = {
    "PLzrFT7CrcDKzurwBd3kKW2vSkH1g8PfsC": "香港疫情追踪",
    "PLzrFT7CrcDKzqJjFo5a4OVmUbW-Iv_g4W": "新聞熱點",
    "PLzrFT7CrcDKxk0YC8p0gmofZ69SBr535F": "財經熱點",
    "PLzrFT7CrcDKwfum4mMLIj_ss7yej2kLhY": "週末故事",
    "PLzrFT7CrcDKyyG1Q6jdu-bKlOQ_7o5SJ2": "新聞追蹤",
    "PLzrFT7CrcDKwcBIKHWtSQ3Hnz-xA-1Beg": "財經觀察站",
    "PLzrFT7CrcDKzTLFnmxNY3cshgppV6IRj1": "路橋點評",
    "PLzrFT7CrcDKxGYEHrnqExnbgS6XN3T38j": "香港簡訊",
    "PLzrFT7CrcDKxBmIk6daYdruBNVen1QY8x": "「希望聽新聞」（粵語）",
    "PLzrFT7CrcDKzmAx8_b-PJVt5AsChQSo4T": "頭頭是道",
    "PLzrFT7CrcDKw6fwKZ8MycOJmfvUXHg80a": "時事解碼",
    "PLzrFT7CrcDKyl4kzjmta-luoLfaWLIUXL": "環球要聞",
    "PLzrFT7CrcDKzQWthZAtsdUmCXRYj0VKI6": "焦點新聞",
    "PLzrFT7CrcDKxbW_UYHWrLKmSiTk26iI6G": "每日要聞",
    "PLzrFT7CrcDKwosbObrScXXRh2AQMfd7bJ": "中國經濟熱點",
    "PLzrFT7CrcDKwWhrP_oUztwvVpkfly82YN": "粵覽新聞",
    "PLzrFT7CrcDKxGGVal3sZIT1tNxt5khXaO": "粵講粵有理",
    "PLzrFT7CrcDKxlkYPylp0GGzmeu824M9F6": "時事熱評",
    "PLy4vsOfbMmQCOd_FZU1-ApAeRnEOeR8de": "法轮功真相",
    "PLy4vsOfbMmQDoruUdSQCspRH_sjhk6t9R": "輕鬆聽新聞",
    "PLy4vsOfbMmQA0emvlfA8KhNoVWGY0D6DA": "精彩回放",
    "PLy4vsOfbMmQBE5CGs-wUoeyP9XFMrCwxk": "熱點直播 專家解讀",
    "PLy4vsOfbMmQCveb4qKZNKqMAgpdrIIhHt": "新聞觀察",
    "PLy4vsOfbMmQBJ3OECiZVryCbmFDMwbgaJ": "時事關心",
    "PLy4vsOfbMmQDClwNJ8luDSUSrzDNmxyu6": "國際風雲",
    "PLy4vsOfbMmQAwjT25VZQOxjj5vDGG_OEC": "北美新聞",
    "PLy4vsOfbMmQATTzUebXKw_gapfxb4IAbY": "每日頭條",
    "PLy4vsOfbMmQCvjXySwbuldIJSNs8zI549": "財經慧眼",
    "PLy4vsOfbMmQB4TsswGdU6l1ie04nkfM9u": "環球看點",
    "PLy4vsOfbMmQAuFTmkhKSy4PHHyEsu6X__": "兩岸要聞",
    "PLy4vsOfbMmQD_0Rg4qUIHGQFO9gVjnBPd": "辛恬面對面",
    "PLy4vsOfbMmQBWOK8zZ93E9ODo2Dw1ABv_": "紅朝禁聞",
    "PLy4vsOfbMmQC15IH7EY3ysN41rruZffQ0": "前方記者 最新報導",
    "PLy4vsOfbMmQBzL2QSSQHPk2yMIL9QrtYT": "美國觀察大選倒計時"}

# https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&part=id&part=status&part=snippet&playlistId=PLy4vsOfbMmQCjNEUVwUFqqoyo52uV2Pd2&playlistId=PLy4vsOfbMmQBzL2QSSQHPk2yMIL9QrtYT&maxResults=50&key=AIzaSyDSmDD_un7dtKMer0oyBZup9fO9gRy1dZ4&videoId=ErKPfiLouUQ

# df = pd.read_csv("./test.csv")
# for i in df['影片 ID ']:

#     request = youtube.playlistItems().list(

#     )
#     response = request.execute()


def getAllPlaylistForSingleVid(playlistIds, vidID):
    for i in playlistIds:
        try:
            request = youtube.playlistItems().list(
                part="snippet",
                maxResults=50,
                playlistId=i,
                videoId=vidID
            )
            response = request.execute()

            if response['items'] != "" and response['items'] != []:
                # print(response['items'][0]['snippet']['playlistId'])
                return response['items'][0]['snippet']['playlistId']

        except:
            pass
            # print("error")

    return ""


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
    # print(data)

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


retrieveAll = False

if retrieveAll == True:

    playlist_json = getVidsForPlaylists(playlistIds)
    with open('playlist_json.json', 'w', encoding='utf-8') as f:
        json.dump(playlist_json, f, ensure_ascii=False, indent=4)
    df = pd.DataFrame.from_dict(playlist_json, orient="index")

    out_df = pd.DataFrame()
    print(df.head())
    out_df['vidIDs'] = df.iloc[:, 0]
    out_df['playlist'] = df.iloc[:, 1]
    out_df.to_csv("playlist.csv")

else:
    # df_playlist = pd.read_csv('./playlist.csv', header=0)
    input_filepath = "./test.csv"
    df = pd.read_csv(input_filepath, header=0)
    print(df.head())
    playlist_json = None
    with open('playlist_json.json') as f:
        playlist_json = json.load(f)

    updatedPlaylist = []
    playlist_names = []
    unknown_count = 0 

    for i in tqdm(df['影片 ID']):

        if i not in playlist_json.keys():
            # exist_playlistID_value = str(
            #     df.loc[df['影片 ID'] == i, '欄目 ID'].iloc[0])
            exist_playlist_value = str(df.loc[df['影片 ID'] == i, '欄目'].iloc[0])
            if exist_playlist_value != "" and exist_playlist_value != None and exist_playlist_value != "nan" and exist_playlist_value != "無欄目":
                # updatedPlaylist.append(exist_playlistID_value)
                playlist_names.append(exist_playlist_value)
                # playlist_json[i] = exist_playlistID_value
                # print("skipped", exist_playlistID_value)
                print("skipped", exist_playlist_value)
                continue

            # print(f"{i} does not exist")
            newPlaylistId = getAllPlaylistForSingleVid(
                playlistIds, i)
            # updatedPlaylist.append(newPlaylistId)
            playlist_name = playlistNames.get(newPlaylistId, "")
            playlist_names.append(playlist_name)

            playlist_json[i] = newPlaylistId
            print("new ", newPlaylistId)

        else:
            updatedPlaylist.append(playlist_json[i])
            playlist_name = playlistNames.get(playlist_json[i], "")
            playlist_names.append(playlist_name)
            print("existing ", playlist_json[i])

    # df["欄目 ID"] = updatedPlaylist
    df['欄目'] = playlist_names

    df.to_csv(input_filepath)

    with open('playlist_json.json', 'w', encoding='utf-8') as f:
        json.dump(playlist_json, f, ensure_ascii=False, indent=4)
