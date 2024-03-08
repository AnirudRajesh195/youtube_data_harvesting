#!/usr/bin/env python
# coding: utf-8

# In[1]:


#YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit



# In[2]:


pip install streamlit


# In[3]:


pip install google-api-python-client


# In[4]:


pip install pymongo


# In[5]:


pip install mysql-connector-python


# In[ ]:





# In[6]:


from googleapiclient.discovery import build
import pandas as pd
import pymongo
import mysql.connector
import streamlit as st


# In[7]:


def Api_connect():
    Api_Id="AIzaSyASGgqF7Hk5YmhVDoNLVfZp1piB4V_yB3E"

    api_service_name="youtube"
    api_version= "v3"

    youtube=build(api_service_name,api_version,developerKey=Api_Id)

    return youtube
youtube=Api_connect()


# In[8]:


chn_id=("UCaFBtetU2RHxwnDRDsNHejA")


# In[9]:


request = youtube.channels().list(
    part="contentDetails,snippet,statistics",
    id="UCaFBtetU2RHxwnDRDsNHejA"
)
response=request.execute()
response


# In[10]:


#Channel name, subscribers, total video count, playlist ID,
#video ID, likes, dislikes, comments
def get_channel_info(channel_id):
    request = youtube.channels().list(
        part="contentDetails,snippet,statistics",
        id=channel_id
    )
    response=request.execute()

    for i in response['items']:
        data= dict(Channel_Name=i["snippet"]["title"],
                  Channel_Id=i["id"],
                  Subscribers=i['statistics']['subscriberCount'],
                  views=i["statistics"]["viewCount"],
                  Total_videos=i["statistics"]["videoCount"],
                  Channel_Description=i["snippet"]["description"],
                  Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data


# In[11]:


channel_details=get_channel_info("UCaFBtetU2RHxwnDRDsNHejA")
channel_details


# In[12]:


def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids


# In[13]:


video_ids=get_videos_ids('UCaFBtetU2RHxwnDRDsNHejA')


# In[14]:


len(video_ids)


# In[15]:


def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
         request=youtube.videos().list(
             part="snippet,contentDetails,statistics",
             id=video_id
         )
         response=request.execute()
         for item in response["items"]:
             data=dict(channel_Name=item['snippet']['channelTitle'],
                      Channel_Id=item['snippet']['channelId'],
                      Video_id=item['id'],
                      Title=item['snippet']['title'],
                      Tags=item['snippet'].get('tags', []),
                      thumbnail=item['snippet']['thumbnails']['default']['url'],
                      Description=item['snippet']['description'],
                      Published_date=item['snippet']['publishedAt'],
                       Duration=item['contentDetails']['duration'],
                       views=item['statistics']['viewCount'],
                       Likes=item['statistics'].get('likeCount'),
                       Comments=item['statistics'].get('commentCount', 0),
                       Favourite_count=item['statistics']['favoriteCount'],
                       Definition=item['contentDetails']['definition'],
                       Caption=item['contentDetails']['caption'],
                      )
             video_data.append(data)
    return video_data


# In[16]:


video_details=get_video_info(video_ids)


# In[17]:


video_details


# In[18]:


def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                         Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                         Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                         Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                         Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                Comment_data.append(data)
    except:
        pass
    return Comment_data


# In[19]:


comment_details=get_comment_info(video_ids)


# In[20]:


comment_details


# In[21]:


#get_playlist_details
def get_playlist_details(channel_id):
    next_page_token=None
    All_data=[]
    while True:
        request=youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response=request.execute()
        for item in  response['items']:
            data=dict(Playlist_id=item['id'],
                      Title=item['snippet']['title'],
                      Channel_Id=item['snippet' ]['channelId'],
                      Channel_Name=item['snippet']['channelTitle'],
                      PublishedAt=item['snippet']['publishedAt'],
                      Video_Count=item['contentDetails']['itemCount'])
            All_data.append(data)
        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break
    return All_data


# In[22]:


playlist_details=get_playlist_details("UCaFBtetU2RHxwnDRDsNHejA")


# In[23]:


len(playlist_details)


# In[24]:


import pandas as pd


# In[25]:


import pymongo
con = pymongo.MongoClient("mongodb://localhost:27017/")
db=con["youtube_data"]


# In[26]:


def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)


    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                     "video_information":vi_details,"comment_information":com_details})
    return "upload completed"


# In[27]:





# In[28]:


insert=channel_details("UCaFBtetU2RHxwnDRDsNHejA")


# In[29]:


insert


# In[30]:


#table creation for channels,playlists,videos,comments
def channel_table():
    import mysql.connector
    mydb = mysql.connector.connect(host='localhost',
                                   user='root',
                                   password='Praveena69@',
                                   database='youtube_data'
                                  )
    cursor = mydb.cursor()

    drop_query='''drop table if exists channel'''
    cursor.execute(drop_query)
    mydb.commit()


    try:
            create_query = '''CREATE TABLE IF NOT EXISTS channel(Channel_Name VARCHAR(100),
                           Channel_Id VARCHAR(80) PRIMARY KEY,
                           Subscribers BIGINT,
                           views BIGINT,
                           Total_videos INT,
                           Channel_Description TEXT,
                           Playlist_Id VARCHAR(80)
                       )'''
            cursor.execute(create_query)
            mydb.commit()

    except:
        print("Channels table already created")


    client = pymongo.MongoClient("mongodb://localhost:27017/youtube_data")

    ch_list=[]
    db = client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)


    for index, row in df.iterrows():
        insert_query='''insert into channel(Channel_Name,
                                            Channel_Id,
                                            Subscribers,
                                            views,
                                            Total_videos,
                                            Channel_Description,
                                            Playlist_Id)

                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Subscribers'],
                row['views'],
                row['Total_videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            mydb.rollback()


# In[31]:


#playlist table creation
def playlist_table():
    mydb = mysql.connector.connect(host='localhost',
                                   user='root',
                                   password='Praveena69@',
                                   database='youtube_data'
                                  )
    cursor = mydb.cursor()

    drop_query='''drop table if exists playlists'''
    cursor.execute(drop_query)
    mydb.commit()



    create_query = '''CREATE TABLE IF NOT EXISTS playlists(Playlist_Id VARCHAR(100) PRIMARY KEY,
                                                           Title VARCHAR(80) ,
                                                           Channel_Id VARCHAR(100),
                                                           Channel_Name VARCHAR(100),
                                                           PublishedAt timestamp,
                                                           Video_Count int
                                                           )'''
    cursor.execute(create_query)
    mydb.commit()

    client = pymongo.MongoClient("mongodb://localhost:27017/youtube_data")
    pl_list=[]
    db = client["youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=pd.DataFrame(pl_list)

    for index, row in df1.iterrows():
        insert_query='''insert into playlists(Playlist_Id,
                                            Title,
                                            Channel_Id,
                                            Channel_Name,
                                            PublishedAt,
                                            Video_Count)

                                            values(%s,%s,%s,%s,%s,%s)'''

        from datetime import datetime

        # Assuming row['PublishedAt'] is a string in the format '2023-02-03T09:58:51Z'
        published_at_str = row['PublishedAt']
        published_at = datetime.strptime(published_at_str, '%Y-%m-%dT%H:%M:%SZ')
        formatted_published_at = published_at.strftime('%Y-%m-%d %H:%M:%S')






        values=(row['Playlist_id'],
                row['Title'],
                row['Channel_Id'],
                row['Channel_Name'],
                formatted_published_at,
                row['Video_Count'])

        cursor.execute(insert_query,values)
        mydb.commit()


# In[32]:


def videos_table():
    mydb = mysql.connector.connect(host='localhost',
                                   user='root',
                                   password='Praveena69@',
                                   database='youtube_data'
                                  )
    cursor = mydb.cursor()

    drop_query='''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()



    create_query = '''CREATE TABLE IF NOT EXISTS videos(channel_Name VARCHAR(1000),
                                                           Channel_Id VARCHAR(1000),
                                                           Video_id VARCHAR(300) PRIMARY KEY,
                                                           Title VARCHAR(1500),
                                                           Tags text,
                                                           thumbnail VARCHAR(2000),
                                                           Description text,
                                                           Published_date timestamp,
                                                           Duration TIME,
                                                           views bigint,
                                                           Likes bigint,
                                                           Comments int,
                                                           Favourite_count int,
                                                           Definition VARCHAR(100),
                                                           Caption VARCHAR(500)
                                                           )'''
    cursor.execute(create_query)
    mydb.commit()

    client = pymongo.MongoClient("mongodb://localhost:27017/youtube_data")
    vi_list=[]
    db = client["youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=pd.DataFrame(vi_list)

    for index, row in df2.iterrows():
        insert_query = '''INSERT IGNORE INTO videos(channel_Name,
                                                    Channel_Id,
                                                    Video_Id,
                                                    Title,
                                                    Tags,
                                                    thumbnail,
                                                    Description,
                                                    Published_date,
                                                    Duration,
                                                    views,
                                                    Likes,
                                                    Comments,
                                                    Favourite_count,
                                                    Definition,
                                                    Caption)
                         VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

        tags_str = ', '.join(row['Tags'])
        from datetime import datetime, timedelta

        # Assuming row['PublishedAt'] is a string in the format '2023-02-03T09:58:51Z'
        published_at_str = row['Published_date']
        published_at = datetime.strptime(published_at_str, '%Y-%m-%dT%H:%M:%SZ')
        formatted_published_at = published_at.strftime('%Y-%m-%d %H:%M:%S')

        # Extracting hours, minutes, and seconds using regular expression
        duration_match = re.match(r'PT(\d+H)?(\d+M)?(\d+S)?', row['Duration'])

        # Check if duration_match is not None before accessing its groups
        if duration_match:
            hours = int(duration_match.group(1)[:-1]) if duration_match.group(1) else 0
            minutes = int(duration_match.group(2)[:-1]) if duration_match.group(2) else 0
            seconds = int(duration_match.group(3)[:-1]) if duration_match.group(3) else 0
        else:
            hours, minutes, seconds = 0, 0, 0

        # Creating a timedelta object
        duration = timedelta(hours=hours, minutes=minutes, seconds=seconds)

        # Formatting the timedelta as 'HH:MM:SS'
        formatted_duration = str(duration)

        values = (row['channel_Name'],
                  row['Channel_Id'],
                  row['Video_id'],
                  row['Title'],
                  tags_str,
                  row['thumbnail'],
                  row['Description'],
                  formatted_published_at,
                  formatted_duration,
                  row['views'],
                  row['Likes'],
                  row['Comments'],
                  row['Favourite_count'],
                  row['Definition'],
                  row['Caption'])

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            mydb.rollback()


# In[33]:


import mysql.connector
import pandas as pd
from datetime import datetime, timedelta
import re

def videos_table():
    # Connect to MySQL
    mydb = mysql.connector.connect(host='localhost', user='root', password='Praveena69@', database='youtube_data')
    cursor = mydb.cursor()

    # Drop and create videos table
    cursor.execute('''DROP TABLE IF EXISTS videos''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS videos (
            channel_Name VARCHAR(1000),
            Channel_Id VARCHAR(1000),
            Video_id VARCHAR(300) PRIMARY KEY,
            Title VARCHAR(1500),
            Tags TEXT,
            thumbnail VARCHAR(2000),
            Description TEXT,
            Published_date TIMESTAMP,
            Duration TIME,
            views BIGINT,
            Likes BIGINT,
            Comments INT,
            Favourite_count INT,
            Definition VARCHAR(100),
            Caption VARCHAR(500)
        )
    ''')
    mydb.commit()

    # Fetch data from MongoDB
    client = pymongo.MongoClient("mongodb://localhost:27017/youtube_data")
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    vi_list = [vi_data["video_information"][i] for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}) for i in
               range(len(vi_data["video_information"]))]
    df2 = pd.DataFrame(vi_list)

    # Insert data into MySQL
    for _, row in df2.iterrows():
        tags_str = ', '.join(row['Tags'])
        published_at = datetime.strptime(row['Published_date'], '%Y-%m-%dT%H:%M:%SZ')
        formatted_published_at = published_at.strftime('%Y-%m-%d %H:%M:%S')

        duration_match = re.match(r'PT(\d+H)?(\d+M)?(\d+S)?', row['Duration'])
        hours = int(duration_match.group(1)[:-1]) if duration_match and duration_match.group(1) else 0
        minutes = int(duration_match.group(2)[:-1]) if duration_match and duration_match.group(2) else 0
        seconds = int(duration_match.group(3)[:-1]) if duration_match and duration_match.group(3) else 0

        duration = timedelta(hours=hours, minutes=minutes, seconds=seconds)
        formatted_duration = str(duration)

        # Insert query
        insert_query = '''
            INSERT IGNORE INTO videos (
                channel_Name, Channel_Id, Video_Id, Title, Tags, thumbnail, Description,
                Published_date, Duration, views, Likes, Comments, Favourite_count, Definition, Caption
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        values = (row['channel_Name'], row['Channel_Id'], row['Video_id'], row['Title'], tags_str,
                  row['thumbnail'], row['Description'], formatted_published_at, formatted_duration,
                  row['views'], row['Likes'], row['Comments'], row['Favourite_count'],
                  row['Definition'], row['Caption'])

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            mydb.rollback()


# In[34]:


def comments_table():
    import mysql.connector
    mydb = mysql.connector.connect(host='localhost',
                                   user='root',
                                   password='Praveena69@',
                                   database='youtube_data'
                                  )
    cursor = mydb.cursor()

    drop_query='''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()


    try:
            create_query = '''CREATE TABLE IF NOT EXISTS comments(Comment_Id VARCHAR(100) PRIMARY KEY,
                                                                   Video_Id VARCHAR(50),
                                                                   Comment_Text TEXT,
                                                                   Comment_Author VARCHAR(150),
                                                                   Comment_Published TIMESTAMP
                                                                   )'''
            cursor.execute(create_query)
            mydb.commit()

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        mydb.rollback()

    client = pymongo.MongoClient("mongodb://localhost:27017/youtube_data")
    com_list=[]
    db = client["youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=pd.DataFrame(com_list)


    for index, row in df3.iterrows():
        insert_query='''insert into comments(Comment_Id,
                                               Video_Id,
                                               Comment_Text,
                                               Comment_Author,
                                               Comment_Published)

                                            values(%s,%s,%s,%s,%s)'''

        from datetime import datetime

        # Assuming row['PublishedAt'] is a string in the format '2023-02-03T09:58:51Z'
        published_at_str = row['Comment_Published']
        published_at = datetime.strptime(published_at_str, '%Y-%m-%dT%H:%M:%SZ')
        formatted_published_at = published_at.strftime('%Y-%m-%d %H:%M:%S')

        values=(row['Comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                formatted_published_at)

        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except mysql.connector.Error as err:
                print(f"Error: {err}")
                mydb.rollback()


# In[35]:


def tables():
    channel_table()
    playlist_table()
    videos_table()
    comments_table()

    return"Tables Created Sucessfully"


# In[36]:


Tables=tables()


# In[37]:


def show_channels_table():
    client = pymongo.MongoClient("mongodb://localhost:27017/youtube_data")

    ch_list=[]
    db = client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)

    return df


# In[38]:


def show_playlists_table():
    client = pymongo.MongoClient("mongodb://localhost:27017/youtube_data")
    pl_list=[]
    db = client["youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=st.dataframe(pl_list)

    return df1


# In[39]:


def show_videos_table():
    client = pymongo.MongoClient("mongodb://localhost:27017/youtube_data")
    vi_list=[]
    db = client["youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=st.dataframe(vi_list)

    return df2


# In[40]:


def show_comments_table():
    client = pymongo.MongoClient("mongodb://localhost:27017/youtube_data")
    com_list=[]
    db = client["youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=st.dataframe(com_list)

    return df3


# In[41]:


#streamlit part
with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skill Take Away")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Intergration")
    st.caption("Data Management using MongoDB and SQL")

channel_id=st.text_input("Enter the channel ID")
if  st.button("collect and store data"):
    ch_ids=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])

    if channel_id in ch_ids:
        st.success("Channel Details of the given channel id already exists")
    else:
        insert=channel_details(channel_id)
        st.success(insert)

if st.button("migrate to sql"):
    Table=tables()
    st.success(Table)

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_playlists_table()

elif show_table=="PLAYLISTS":
    show_playlists_table()

elif show_table=="VIDEOS":
    show_playlists_table()

elif show_table=="COMMENTS":
    show_playlists_table()


# In[42]:


#SQL Connection

mydb = mysql.connector.connect(host='localhost',
                               user='root',
                               password='Praveena69@',
                               database='youtube_data'
                              )
cursor = mydb.cursor()

question=st.selectbox("Select your question",("1. All the videos and the channel name",
                                                "2. Channels with most number of videos",
                                                "3. 10 most viewed videos",
                                                "4. Comments in each videos",
                                                "5. Videos with highest likes",
                                                "6. Likes of all videos",
                                                "7. Views of each channel",
                                                "8. Videos published in the year of 2022",
                                                "9. Average duration of all videos in each channel",
                                                "10. Videos with highest number of comments"))


if question=="1. All the videos and the channel name":
    query1='''select title as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["video_title","channel_name"])
    mydb.commit()
    st.write(df)

elif question=="2. Channels with most number of videos":
    query2='''select channel_name as channelname,total_videos as no_videos from channel
                order by total_videos desc'''
    cursor.execute(query2)
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel_name","No of videos"])
    mydb.commit()
    st.write(df2)

elif question=="3. 10 most viewed videos":
    query3='''select views as views,channel_name as channelname,title as videotitle from videos
                where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channel_name","videotitle"])
    mydb.commit()
    st.write(df3)

elif question=="4. Comments in each videos":
    query4='''select comments as no_comments,title as videotitle from videos where comments is not null'''
    cursor.execute(query4)
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["No of comments","videotitle"])
    mydb.commit()
    st.write(df4)

elif question=="5. Videos with highest likes":
    query5='''select title as videotitle,channel_name as channelname,Likes as likecount
                from videos where likes is not null order by likes desc'''
    cursor.execute(query5)
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    mydb.commit()
    st.write(df5)

elif question=="6. Likes of all videos":
    query6='''select Likes as likecount,title as videotitle from videos'''
    cursor.execute(query6)
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likecount","videotitle"])
    mydb.commit()
    st.write(df6)

elif question=="7. Views of each channel":
    query7='''select Channel_name as channelname,views as totalviews from channel'''
    cursor.execute(query7)
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channelname","totalviews"])
    mydb.commit()
    st.write(df7)

elif question=="8. Videos published in the year of 2022":
    query8='''select title as video_title,Published_date as videorelease,channel_name as channelname from videos
                where extract(year from Published_date)=2022'''
    cursor.execute(query8)
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["video_title","Published_date","channelname"])
    mydb.commit()
    st.write(df8)

elif question=="9. Average duration of all videos in each channel":
    query9='''select channel_name as channelname,SEC_TO_TIME(AVG(TIME_TO_SEC(duration))) AS averageduration from videos group by channel_name'''
    cursor.execute(query9)
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","averageduration"])

    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df1=pd.DataFrame(T9)
    mydb.commit()
    st.write(df1)

elif question=="10. Videos with highest number of comments":
    query10='''select title as videotitle,channel_name as channelname, comments as comments from videos where comments is
                not null order by comments desc'''
    cursor.execute(query10)
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["videotitle","channelname","comments"])
    mydb.commit()
    st.write(df10)


# In[43]:


import pandas as pd
import mysql.connector
mydb = mysql.connector.connect(host='localhost',
                               user='root',
                               password='Praveena69@',
                               database='youtube_data'
                              )
cursor = mydb.cursor()
#"6. Likes of all videos",
                                                #"7. Views of each channel",
                                                #"8. Videos published in the year of 2022",
                                                #"9. Average duration of all videos in each channel",
                                                #"10. Videos with highest number of comments"))
#elif question=="10. Videos with highest number of comments":
query10='''select title as videotitle,channel_name as channelname, comments as comments from videos where comments is
            not null order by comments desc'''
cursor.execute(query10)
t10=cursor.fetchall()
df10=pd.DataFrame(t10,columns=["videotitle","channelname","comments"])
df10



# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[44]:


client = pymongo.MongoClient("mongodb://localhost:27017/youtube_data")
com_list=[]
db = client["youtube_data"]
coll1=db["channel_details"]
for com_data in coll1.find({},{"_id":0,"comment_information":1}):
    for i in range(len(com_data["comment_information"])):
        com_list.append(com_data["comment_information"][i])
df3=pd.DataFrame(com_list)


# In[45]:


for index, row in df3.iterrows():
    insert_query='''insert into comments(Comment_Id,
                                           Video_Id,
                                           Comment_Text,
                                           Comment_Author,
                                           Comment_Published)

                                        values(%s,%s,%s,%s,%s)'''

    from datetime import datetime

    # Assuming row['PublishedAt'] is a string in the format '2023-02-03T09:58:51Z'
    published_at_str = row['Comment_Published']
    published_at = datetime.strptime(published_at_str, '%Y-%m-%dT%H:%M:%SZ')
    formatted_published_at = published_at.strftime('%Y-%m-%d %H:%M:%S')

    values=(row['Comment_Id'],
            row['Video_Id'],
            row['Comment_Text'],
            row['Comment_Author'],
            formatted_published_at)

    try:
        cursor.execute(insert_query,values)
        mydb.commit()
    except mysql.connector.Error as err:
            print(f"Error: {err}")
            mydb.rollback()


# In[46]:


client = pymongo.MongoClient("mongodb://localhost:27017/youtube_data")
vi_list=[]
db = client["youtube_data"]
coll1=db["channel_details"]
for vi_data in coll1.find({},{"_id":0,"video_information":1}):
    for i in range(len(vi_data["video_information"])):
        vi_list.append(vi_data["video_information"][i])
df2=pd.DataFrame(vi_list)


# In[47]:


import re
for index, row in df2.iterrows():
        insert_query='''insert into videos(channel_Name,
                                               Channel_Id,
                                               Video_Id,
                                               Title,
                                               Tags,
                                               thumbnail,
                                               Description,
                                               Published_date,
                                               Duration,
                                               views,
                                               Likes,
                                               Comments,
                                               Favourite_count,
                                               Definition,
                                               Caption)

                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''


        tags_str = ', '.join(row['Tags'])
        from datetime import datetime, timedelta

        # Assuming row['PublishedAt'] is a string in the format '2023-02-03T09:58:51Z'
        published_at_str = row['Published_date']
        published_at = datetime.strptime(published_at_str, '%Y-%m-%dT%H:%M:%SZ')
        formatted_published_at = published_at.strftime('%Y-%m-%d %H:%M:%S')

    #for duration
        duration_str = row['Duration']
        pattern = r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?'

# Match the pattern against the duration string
        match = re.match(pattern, duration_str)

# Extract hours, minutes, and seconds
        hours = int(match.group(1)) if match.group(1) else 0
        minutes = int(match.group(2)) if match.group(2) else 0
        seconds = int(match.group(3)) if match.group(3) else 0
        

    # Creating a timedelta object
        duration = timedelta(hours=hours, minutes=minutes, seconds=seconds)

    # Formatting the timedelta as 'HH:MM:SS'
        formatted_duration = str(duration)


        values=(row['channel_Name'],
                        row['Channel_Id'],
                        row['Video_id'],
                        row['Title'],
                        tags_str,
                        row['thumbnail'],
                        row['Description'],
                        formatted_published_at,
                        formatted_duration,
                        row['views'],
                        row['Likes'],
                        row['Comments'],
                        row['Favourite_count'],
                        row['Definition'],
                        row['Caption'])
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            mydb.rollback()


# In[48]:


for index, row in df2.iterrows():
    insert_query = '''INSERT IGNORE INTO videos(channel_Name,
                                                Channel_Id,
                                                Video_Id,
                                                Title,
                                                Tags,
                                                thumbnail,
                                                Description,
                                                Published_date,
                                                Duration,
                                                views,
                                                Likes,
                                                Comments,
                                                Favourite_count,
                                                Definition,
                                                Caption)
                     VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

    tags_str = ', '.join(row['Tags'])
    from datetime import datetime, timedelta

    # Assuming row['PublishedAt'] is a string in the format '2023-02-03T09:58:51Z'
    published_at_str = row['Published_date']
    published_at = datetime.strptime(published_at_str, '%Y-%m-%dT%H:%M:%SZ')
    formatted_published_at = published_at.strftime('%Y-%m-%d %H:%M:%S')

    # Extracting hours, minutes, and seconds using regular expression
    duration_match = re.match(r'PT(\d+H)?(\d+M)?(\d+S)?', row['Duration'])

    # Check if duration_match is not None before accessing its groups
    if duration_match:
        hours = int(duration_match.group(1)[:-1]) if duration_match.group(1) else 0
        minutes = int(duration_match.group(2)[:-1]) if duration_match.group(2) else 0
        seconds = int(duration_match.group(3)[:-1]) if duration_match.group(3) else 0
    else:
        hours, minutes, seconds = 0, 0, 0

    # Creating a timedelta object
    duration = timedelta(hours=hours, minutes=minutes, seconds=seconds)

    # Formatting the timedelta as 'HH:MM:SS'
    formatted_duration = str(duration)

    values = (row['channel_Name'],
              row['Channel_Id'],
              row['Video_id'],
              row['Title'],
              tags_str,
              row['thumbnail'],
              row['Description'],
              formatted_published_at,
              formatted_duration,
              row['views'],
              row['Likes'],
              row['Comments'],
              row['Favourite_count'],
              row['Definition'],
              row['Caption'])

    try:
        cursor.execute(insert_query, values)
        mydb.commit()
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        mydb.rollback()


# In[49]:


for index, row in df2.iterrows():
    insert_query = '''INSERT INTO videos(
                          channel_Name,
                          Channel_Id,
                          Video_id,
                          Title,
                          Tags,
                          thumbnail,
                          Description,
                          Published_date,
                          Duration,
                          views,
                          Likes,
                          Comments,
                          Favourite_count,
                          Definition,
                          Caption
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''

    values = (
        row['channel_Name'],
        row['Channel_Id'],
        row['Video_id'],
        row['Title'],
        row['Tags'],
        row['thumbnail'],
        row['Description'],
        row['Published_date'],
        row['Duration'],
        row['views'],
        row['Likes'],
        row['Comments'],
        row['Favourite_count'],
        row['Definition'],
        row['Caption']
    )

    try:
        cursor.execute(insert_query, values)
        mydb.commit()
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        mydb.rollback()


# In[ ]:


client = pymongo.MongoClient("mongodb://localhost:27017/youtube_data")

ch_list=[]
db = client["youtube_data"]
coll1=db["channel_details"]
for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
    ch_list.append(ch_data["channel_information"])
df=pd.DataFrame(ch_list)


# In[ ]:


client = pymongo.MongoClient("mongodb://localhost:27017/youtube_data")
pl_list=[]
db = client["youtube_data"]
coll1=db["channel_details"]
for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
    for i in range(len(pl_data["playlist_information"])):
        pl_list.append(pl_data["playlist_information"][i])
df1=pd.DataFrame(pl_list)


# In[ ]:





# In[ ]:


for index, row in df1.iterrows():
    insert_query='''insert into playlists(Playlist_Id,
                                        Title,
                                        Channel_Id,
                                        Channel_Name,
                                        PublishedAt,
                                        Video_Count)

                                        values(%s,%s,%s,%s,%s,%s)'''

    from datetime import datetime

    # Assuming row['PublishedAt'] is a string in the format '2023-02-03T09:58:51Z'
    published_at_str = row['PublishedAt']
    published_at = datetime.strptime(published_at_str, '%Y-%m-%dT%H:%M:%SZ')
    formatted_published_at = published_at.strftime('%Y-%m-%d %H:%M:%S')






    values=(row['Playlist_id'],
            row['Title'],
            row['Channel_Id'],
            row['Channel_Name'],
            formatted_published_at,
            row['Video_Count'])

    cursor.execute(insert_query,values)
    mydb.commit()


# In[ ]:


for index, row in df.iterrows():
    insert_query='''insert into channels(Channel_Name,
                                        Channel_Id,
                                        Subscribers,
                                        views,
                                        Total_videos,
                                        Channel_Description,
                                        Playlist_Id)

                                        values(%s,%s,%s,%s,%s,%s,%s)'''
    values=(row['Channel_Name'],
            row['Channel_Id'],
            row['Subscribers'],
            row['views'],
            row['Total_videos'],
            row['Channel_Description'],
            row['Playlist_Id'])
    try:
        cursor.execute(insert_query,values)
        mydb.commit()
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        mydb.rollback()


# In[ ]:


insert_query = '''INSERT INTO channel(Channel_Name,
                                    Channel_Id,
                                    Subscribers,
                                    views,
                                    Total_videos,
                                    Channel_Description,
                                    Playlist_Id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)'''

values = []
for index, row in df.iterrows():
    value_tuple = (row['Channel_Name'],
                   row['Channel_Id'],
                   row['Subscribers'],  # Corrected typo here
                   row['views'],
                   row['Total_videos'],
                   row['Channel_Description'],
                   row['Playlist_Id'])
    values.append(value_tuple)

try:
    # Use executemany for bulk insert
    cursor.executemany(insert_query, values)
    mydb.commit()
except mysql.connector.Error as err:
    print(f"Error: {err}")
    mydb.rollback()


# In[ ]:





# In[ ]:




