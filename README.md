# YouTube API Python
This repository contains Python scripts used to retrieve YouTube data.

=============================
- [YouTube API Python](#youtube-api-python)
  - [Installation](#installation)
  - [Usage](#usage)
    - [Run FinalYTAnalyticsV2.0.py:](#run-finalytanalyticsv20py)
    - [Run YTAnalyticsV3.0.py:](#run-ytanalyticsv30py)
  - [Environment Variables](#environment-variables)
  - [Authors](#authors)
  - [Support](#support)


----------------------------------
## Installation

1. Create virtual environment 
```python3 -m venv env```

2. Activate virtual environment 
(for Windows): ```.\env\Scripts\activate```
(for mac): ```source env/bin/activate```
(for Linux): ```. ./env/bin/activate```

3. Upgrade pip
```python3 -m pip install --upgrade pip```

4. Install required libraries 
```pip install -r requirements.txt```

5. Create .env file in the folder (same location as requirements.txt)
See [Environment Variables](#environment-variables)


=======

## Usage

### Run FinalYTAnalyticsV2.0.py:
  
```pip install -r requirements.txt```


### Run YTAnalyticsV3.0.py:
  
1. Activate virtual environment:

```source ./venv/bin/activate```
  
2. Install requirements/venv.txt:

```pip install -r requirements/venv.txt```

## Environment Variables

To run this project, you will need to add the following environment variables to your .env file

`API_KEY`  
Use for any code that utilizes YouTube Analytics API or YouTube Reporting API. Can be found in Google Developers Console.   
`API_KEY2`  
Back up API_KEY. Optional. Used in case daily quota limit is exceeded.  
`SOH_USERNAME`  
Username for Google Account. Only use for selenium_approve.py. Use with caution.   
`PASSWORD`  
Password for SOH_USERNAME. Used only for selenium_approve.py  
`CHANNEL_NAMES`  
Python Dictionary. Keys = channel ID, value = channel name. Set value in channel_config.json, file included as example.      
`CHANNEL_IDS`  
Python Dictionary. Keys = channel name, value = channel ID.Set value in channel_config.json, file included as example.      
`UPLOADS_PLAYLIST_IDS`  
Python Dictionary. Keys = channel name, value = channel uploads playlist ID. Set value in channel_config.json, file included as example.    

## Authors

- [@Anna33625](https://www.github.com/Anna33625)
- [@Ianyliu](https://www.github.com/Ianyliu)
- [@yuqianlucy](https://www.github.com/yuqianlucy)

## Support

For support, email frank.lee@soundofhope.org or data.analysis@soundofhope.org.
