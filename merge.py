import sqlite3
import pandas as pd

# Create a SQL connection to our SQLite database
con = sqlite3.connect("user_datailed_20191219-20201231_new.db")

cur = con.cursor()

def get_user(cur: sqlite3.Cursor, user_id:str):
    result = []
    for i in cur.execute(f'SELECT * FROM users where user_id is \'{user_id}\''):
        result.append(i)
    return result
    

# read by default 1st sheet of an excel file
dataframe1 = pd.read_excel("WWDB_new_2.xlsx")

for i, player in enumerate(dataframe1["playerid"]):
    print("---------------------------------")
    print('[Info]: ', player)
    db_data = get_user(cur=cur, user_id=player)
    print('[Info]: ',db_data)
    if db_data == []:
        pass
    else:
        spent = db_data[0][1] + (0 if dataframe1["Total spent $"][i] else dataframe1["Total spent $"][i])
        purchase = db_data[0][2] + (0 if dataframe1["# purchases"][i] else dataframe1["# purchases"][i])
        video = db_data[0][3] + (0 if dataframe1["# of times watched video ad"][i] else dataframe1["# of times watched video ad"][i])
        event = db_data[0][4] + (0 if dataframe1["total events"][i] else dataframe1["total events"][i])
        session = db_data[0][5] + (0 if dataframe1["Total sessions"][i] else dataframe1["Total sessions"][i])
        if spent != 0:
            dataframe1.loc[i, "Total spent $"] = spent
        if purchase != 0:
            dataframe1.loc[i, "# purchases"] = purchase
        if video != 0:
            dataframe1.loc[i, "# of times watched video ad"] = video
        if event != 0:
            dataframe1.loc[i, "total events"] = event
        if session != 0:
            dataframe1.loc[i, "Total sessions"] = session
    # if i == 10000:
    #     break

dataframe1.to_excel("WWDB_new_2.xlsx", index=False)

con.close()