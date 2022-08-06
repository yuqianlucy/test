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
