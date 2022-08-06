from msilib.schema import CheckBox
import tkinter as tk
from tkinter import *


def selectChannel(channel_list: dict):
    root = tk.Tk()

    # config the root window
    root.geometry('500x500')
    root.title('Select Channels')

    def var_states():
        print("male: %d,\nfemale: %d" % (var1.get(), var2.get()))

    def getChannels():
        pass

    Label(root, text="Please select your channel").grid(row=0, column=3)

    channel_selected = []   
    for index, dict_key in enumerate(channel_list.keys()):
        channel_name = channel_list[dict_key]
        channel_bool = BooleanVar()
        channel_checkbox = Checkbutton(text=channel_name, variable = channel_bool)
        channel_checkbox.grid(row=index+1, column=1, pady=20, padx=5)

        #Save checkbox to list
        channel_selected.append()
        

    # var1 = IntVar()
    # Checkbutton(text="male", variable=var1).grid(row=1)
    # var2 = IntVar()
    # Checkbutton(text="female", variable=var2).grid(row=2)
    Button(text='Quit', command=root.quit).grid(row=index+2, pady=4)
    Button(text='Show', command=var_states).grid(row=index+3,  pady=4)

    def on_closing():
        global MONTH_SELECTED
        # save the channel selected to a global variable named MONTH_SELECTED
        # MONTH_SELECTED = selected_month.get()
        root.destroy()  # close the Tkinter root
        return(f"{channel_bool}")

    root.protocol("WM_DELETE_WINDOW", on_closing)

    root.mainloop()

ch =  {
    "UCCdWF5GML4ai-DVSp0Tgyxg": "希望之聲粵語頻道",
    "UCk89pEd76qutMB08hVSY49Q": "希望之聲TV",
    "UCizGWTffp1z_d4oU_gpwl-Q": "頭頭是道"
}
selectChannel(ch)
