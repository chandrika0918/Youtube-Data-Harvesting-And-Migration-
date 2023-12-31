import streamlit as st
import googleapiclient.discovery
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import mysql.connector
from mysql.connector import Error
import pandas as pd


api_key = "AIzaSyCry8QFTcdYrp_x53omIkmsv9dWHHdQOdM"
api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
uri = "mongodb+srv://iamchandrika92:1234abcd@cluster0.aqrby2f.mongodb.net/?retryWrites=true&w=majority"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))
# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)
# Create Database and Collection in mongodb
client = MongoClient("mongodb+srv://iamchandrika92:1234abcd@cluster0.aqrby2f.mongodb.net/?retryWrites=true&w=majority")
database = client.Youtube_Datas
collection = database.Channel_Information


# Getting Channel Details
def get_channel_details(a): 
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=a
    )
    response = request.execute()

    ch_details = dict(
        ch_id = response['items'][0]['id'],
        ch_title = response['items'][0]['snippet']['title'],
        publish_at = response['items'][0]['snippet']['publishedAt'],
        description = response['items'][0]['snippet']['description'],
        sb_count = response['items'][0]['statistics']['subscriberCount'],
        vd_count = response['items'][0]['statistics']['videoCount'],
        view_count = response['items'][0]['statistics']['viewCount'],
        paylist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    )

    return ch_details

# Getting Play List Details
def get_playlist_details(b):
    play_list_details = []
    channel_id = b

    request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=25
        )
    response = request.execute()
    for i in range(len(response['items'])):
        playlist_details = dict(
            play_list_id = response['items'][i]['id'],
            channel_id =  response['items'][i]['snippet']['channelId'],                     
            play_list_name = response['items'][i]['snippet']['title']
        )
        play_list_details.append(playlist_details)

    return play_list_details

# Getting video IDs
def Get_video_Ids(d):
    next_page_token = None
    video_ids = []
    pl_list_id = get_channel_details(d)
    while True:
        request = youtube.playlistItems().list(
            part="snippet,contentDetails",
            maxResults=1,
            playlistId=pl_list_id['paylist_id'],
            pageToken = next_page_token
        )
        response1 = request.execute()

        for i in range(len(response1['items'])):        
            video_ids.append(str(response1['items'][i]['snippet']['resourceId']['videoId']))

        next_page_token = response1.get('nextPageToken')
        if next_page_token is None:
            break

    return ( video_ids )

# Getting Video Details
def Get_video_details(ch_id,c):
    videos = []
    pla_list_id = get_playlist_details(ch_id)
    for video_id in c:
        request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=video_id
            )
        response = request.execute()

        for i in range(len(response['items'])):
            vd_details = dict(
                vd_id = response['items'][i]['id'],
                playlist_Id = pla_list_id[0]['play_list_id'],
                vd_title = response['items'][i]['snippet']['title'],
                vd_publish_at = response['items'][i]['snippet']['publishedAt'],
                vd_description = response['items'][i]['snippet']['description'],
                vd_view_count = response.get('viewCount'),
                vd_like_count = response.get('likeCount'),
                vd_comment_count = response.get('commentCount'),
                vd_duration = response['items'][i]['contentDetails']['duration'],
                vd_tags = response.get('tags'),
                vd_favorite_Count = response.get('favoriteCount'),
                vd_thumbnail = response['items'][i]['snippet']['thumbnails']['default']['url'],
                vd_caption_status = response.get('caption'),
            )
            videos.append(vd_details)

    return videos

# Getting comment Threads Details
def get_comment_details(d):
    comment_details = []
    try:
        for video_id in d:
            request = youtube.commentThreads().list(
                        part='snippet, replies',
                        videoId=video_id,
                        maxResults = 50
                    )
            response = request.execute()
            for i in range(len(response['items'])):
                cm_data = dict(
                    comment_id = response['items'][i]['snippet']['topLevelComment']['id'],
                    video_id = response['items'][i]['snippet']['videoId'],
                    comment_text = response['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay'],
                    comment_author = response['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    comment_published_date = response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt']
                )
                comment_details.append(cm_data)
    except:
        pass
    return comment_details
def Get_Channels_Infos(ch_id):
    channel_info = get_channel_details(ch_id)
    play_list_info = get_playlist_details(ch_id)
    video_ids = Get_video_Ids(ch_id)
    video_info = Get_video_details(ch_id,video_ids)
    comments_info = get_comment_details(video_ids)
    collection.insert_one({
                        "channel_information":channel_info,
                        "playlist_information": play_list_info,
                        "video_information" : video_info,
                        "comments_information": comments_info
                    })
    return "Uploded Successfully"
get_input = st.text_input(label = "Enter The Channel ID", placeholder="Channel ID")
extract = st.button("Extract Here")
if extract:
    if get_input:
        channel_info = get_channel_details(get_input)
        st.write(channel_info)
    else:
        st.write("Please Enter The channnel Id")
Transfer = st.button("Upload To MonogoDb")
if Transfer:
    if get_input:
        Insert = Get_Channels_Infos(get_input)
        st.write(Insert)    
    else:
        st.write("Please Enter The channnel Id")
# FUNCTION TO GET CHANNEL NAMES FROM MONGODB
def Get_channel_names():
    ch_names = []
    for i in database.Channel_Information.find():
        ch_names.append(i['channel_information']['ch_title'])
    return ch_names
# Insert Selected channel name details 
channel_names = Get_channel_names()
selected_name = st.selectbox('Select Channel Name',options=channel_names)
mydb = mysql.connector.connect(
                                    host="localhost",
                                    user="root",
                                    password="",
                                    database='youtube_Db'
                                )
mycursor = mydb.cursor()  
def Insert_channel_details():
    ch_list = collection.find({"channel_information.ch_title": selected_name},{"_id":0,"playlist_information":0,"video_information":0,"comments_information":0})
    ch_df = pd.DataFrame(list(ch_list))
    # Drop Table If Exisits 
    # drop_query = '''DROP TABLE If EXISTS Channel_Details'''
    # mycursor.execute(drop_query)
    # mydb.commit()
    
    #create Table in sql db
    ch_create_query = '''CREATE TABLE IF NOT EXISTS Channel_Details (ch_title VARCHAR(100) ,\
                                                                    ch_id VARCHAR(100) PRIMARY KEY,\
                                                                    publish_at VARCHAR(255),\
                                                                    description text,\
                                                                    sb_count bigint,\
                                                                    vd_count bigint,\
                                                                    view_count bigint,\
                                                                    paylist_id VARCHAR(100))'''
    mycursor.execute(ch_create_query)
    mydb.commit()
    # Insert Values to sql table
    for _,row in ch_df.iterrows():
        ch_insert_query = '''INSERT INTO Channel_Details(ch_title, ch_id, publish_at, description, sb_count,\
                                                        vd_count, view_count, paylist_id) VALUES(%s, %s, %s, %s,\
                                                        %s, %s, %s, %s)'''
        values = (
                row['channel_information']['ch_title'],
                row['channel_information']['ch_id'],
                row['channel_information']['publish_at'],
                row['channel_information']['description'],
                row['channel_information']['sb_count'],
                row['channel_information']['vd_count'],
                row['channel_information']['view_count'],
                row['channel_information']['paylist_id']
            )
        mycursor.execute(ch_insert_query,values)
        mydb.commit()
def Insert_playlist_details():
    pl_list = collection.find_one({"channel_information.ch_title": selected_name},{"_id":0,"channel_information":0,"video_information":0,"comments_information":0})
    new_pl_list = []
    for i in range(len(pl_list['playlist_information'])):
        new_pl_list.append(pl_list['playlist_information'][i])
    pl_df1 = pd.DataFrame(list(new_pl_list))
    # Drop Table If Exisits 
    # drop_query = '''DROP TABLE If EXISTS playlist_details'''
    # mycursor.execute(drop_query)
    # mydb.commit()
    
    #create Table in sql db
    pl_create_query = '''CREATE TABLE IF NOT EXISTS playlist_details (
                                                                    play_list_id VARCHAR(255) PRIMARY KEY,
                                                                    channel_id VARCHAR(255),
                                                                    play_list_name VARCHAR(255),
                                                                    FOREIGN KEY (channel_id)REFERENCES Channel_Details(ch_id)
                                                                    )'''
    mycursor.execute(pl_create_query)
    mydb.commit()
    # Insert Values to sql table
    for _,row in pl_df1.iterrows():
        pl_insert_query = '''INSERT INTO playlist_details(play_list_id, channel_id, play_list_name) VALUES(%s, %s, %s)'''
        values = (
                row['play_list_id'],
                row['channel_id'],
                row['play_list_name']
            )
        mycursor.execute(pl_insert_query,values)
        mydb.commit()
def Insert_video_details():
    vd_list = collection.find_one({"channel_information.ch_title": selected_name},{"_id":0,"channel_information":0,"playlist_information":0,"comments_information":0})
    new_vd_list = []
    for i in range(len(vd_list['video_information'])):
        new_vd_list.append(vd_list['video_information'][i])
    pl_df2 = pd.DataFrame(list(new_vd_list))
    # Drop Table If Exisits 
    # drop_query = '''DROP TABLE If EXISTS video_data'''
    # mycursor.execute(drop_query)
    # mydb.commit()
    #create Table in sql db
    vd_create_query = '''CREATE TABLE IF NOT EXISTS video_data (
                                                                    vd_id VARCHAR(255) PRIMARY KEY,
                                                                    playlist_Id VARCHAR(255),
                                                                    vd_title VARCHAR(255),
                                                                    vd_publish_at DATETIME,
                                                                    vd_description TEXT,
                                                                    vd_view_count INT,
                                                                    vd_like_count INT,
                                                                    vd_comment_count INT,
                                                                    vd_duration VARCHAR(155),
                                                                    vd_tags VARCHAR(255),
                                                                    vd_favorite_Count INT,
                                                                    vd_thumbnail VARCHAR(255),
                                                                    vd_caption_status VARCHAR(255),
                                                                    FOREIGN KEY (playlist_Id)REFERENCES playlist_details(play_list_id)
                                                                    )'''
    mycursor.execute(vd_create_query)
    mydb.commit()
    # Insert Values to sql table
    for _,row in pl_df2.iterrows():
        vd_insert_query = '''INSERT INTO video_data(vd_id, playlist_Id, vd_title,vd_publish_at,vd_description,\
                                                        vd_view_count,vd_like_count,vd_comment_count,vd_duration,\
                                                        vd_tags,vd_favorite_Count,vd_thumbnail,vd_caption_status)\
                                                        VALUES(%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s)'''
        values = (
                row['vd_id'],
                row['playlist_Id'],
                row['vd_title'],
                row['vd_publish_at'],
                row['vd_description'],
                row['vd_view_count'],
                row['vd_like_count'],
                row['vd_comment_count'],
                row['vd_duration'],
                row['vd_tags'],
                row['vd_favorite_Count'],
                row['vd_thumbnail'],
                row['vd_caption_status']
            )
        mycursor.execute(vd_insert_query,values)
        mydb.commit()
def Insert_comment_details():
    cm_list = collection.find_one({"channel_information.ch_title": selected_name},{"_id":0,"channel_information":0,"playlist_information":0,"video_information":0})
    new_cm_list = []
    for i in range(len(cm_list['comments_information'])):
        new_cm_list.append(cm_list['comments_information'][i])
    cm_df = pd.DataFrame(list(new_cm_list))
    
    #create Table in sql db
    cm_create_query = '''CREATE TABLE IF NOT EXISTS Comment_Details (
                                                                    comment_id VARCHAR(255) PRIMARY KEY,
                                                                    video_id VARCHAR(255),
                                                                    comment_text VARCHAR(255),
                                                                    comment_author VARCHAR(255),
                                                                    comment_published_date DATETIME,
                                                                    FOREIGN KEY (video_id)REFERENCES video_data(vd_id)
                                                                    )'''
    mycursor.execute(cm_create_query)
    mydb.commit()
    # Print or log video_id values
    problematic_video_ids = []
    # Insert Values to sql table
    for _,row in cm_df.iterrows():
        try:
            cm_insert_query = '''INSERT INTO Comment_Details(comment_id, video_id, comment_text,comment_author,comment_published_date) VALUES(%s, %s, %s,%s, %s)'''
            values = (
                    row['comment_id'],
                    row['video_id'],
                    row['comment_text'],
                    row['comment_author'],
                    row['comment_published_date']
                )
            mycursor.execute(cm_insert_query,values)
            mydb.commit()
        except Exception as e:
            # Print or log the problematic video_id
            st.write(f"Error inserting record with video_id: {row['video_id']}")
            problematic_video_ids.append(row['video_id'])
            st.write(e)

    # Print or log all problematic video_id values
    st.write("Problematic video_id values:", problematic_video_ids)
st.subheader("click here to migrate the data to sql tables")
if st.button("Migrate"):
    try:
        Insert_channel_details()
        Insert_playlist_details()
        Insert_video_details()
        Insert_comment_details()
        st.success("Transformation to MySQL Successful !!")
    except Error as e:
        st.write(e)

