import datetime
from analytix import YouTubeAnalytics
from analytix.youtube.analytics.verify.rtypes import ReportType

client = YouTubeAnalytics.from_file("./YouTubeTEST/SOHclient_secret.json")
client.authorise()

#Traffic Sources per day
report = client.retrieve(start_date=datetime.date(2021,12,1), #retrieve all data starting from 2021-12-01
         dimensions=['day','insightTrafficSourceType'], #retrieve data for all videos
        metrics=['views', 'estimatedMinutesWatched'] , #list of metrics to retrieve data for
        max_results=200, # set maximum number of results (has to be less than 200)
        sort_by=['-views'], 
        # filters={'video':'3J52QDZkhH8'},
        include_historical_data=True) 
df = report.to_dataframe()
df.to_csv('Traffic Sources Day.csv')

#All time traffic sources
report = client.retrieve(start_date=datetime.date(2021,12,1), #retrieve all data starting from 2021-12-01
         dimensions=['insightTrafficSourceType'], #retrieve data for all videos
        metrics=['views', 'estimatedMinutesWatched'] , #list of metrics to retrieve data for
        max_results=200, # set maximum number of results (has to be less than 200)
        sort_by=['-views'], 
        # filters={'video':'3J52QDZkhH8'},
        include_historical_data=True) 
df = report.to_dataframe()
df.to_csv('Traffic Sources.csv')

#List of traffic types and what they mean 
# Subscriber: Traffic from homepage/home screen, subscription, and other browsing features 
# Related video:videos suggested 
# Yt channel: youtube channel pages 
#yt search: 
#notification:
#external url:
#yt other page: "other" category
#no link other: from direct url entry 
#playlist:
# end screen:
#yt playlist page:
#annotation:
#hashtags:
#shorts:
#advertising: 