# importing all files  from tkinter
from tkinter import * 
# from tkinter import ttk
  
# import only asksaveasfile from filedialog
# which is used to save file in any extension
from tkinter.filedialog import askdirectory, asksaveasfile
  
# root = Tk()
# root.geometry('200x150')
  
# function to call when user press
# the save button, a filedialog will
# open and ask to save file
def save():
    files = [('All Files', '*.*'), 
             ('Python Files', '*.py'),
             ('Text Document', '*.txt')]
    file = asksaveasfile(filetypes = files, defaultextension = files)
    print(file)

save()

########################################################
# So far this is the best thing to use when asking where to save the file at 
def askDirectory():
    file = askdirectory()
    print(file)

askDirectory()
  
# btn = ttk.Button(root, text = 'Save', command = lambda : save())
# btn.pack(side = TOP, pady = 20)
  
# mainloop()