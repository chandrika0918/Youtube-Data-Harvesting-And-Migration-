import streamlit as st
import pandas as pd
import re
from datetime import timedelta
import googleapiclient.discovery
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import mysql.connector
from mysql.connector import Error
from streamlit_option_menu import option_menu

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

mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database='youtube_Db'
    )
mycursor = mydb.cursor() 

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
            playlist_Id=pla_list_id[0]['play_list_id'],
            channel_title = response['items'][i]['snippet']['channelTitle'],
            vd_title = response['items'][i]['snippet']['title'],
            vd_publish_at = response['items'][i]['snippet']['publishedAt'],
            vd_description = response['items'][i]['snippet']['description'],
            vd_view_count = response['items'][i]['statistics'].get('viewCount'),
            vd_like_count = response['items'][i]['statistics'].get('likeCount'),
            vd_dislike_count = response['items'][i]['statistics'].get('vd_dislike_count', '0'),
            vd_comment_count = response['items'][i]['statistics'].get('commentCount'),
            vd_duration = response['items'][i]['contentDetails']['duration'],
            vd_tags = response['items'][i]['snippet'].get('tags'),
            vd_favorite_Count = response['items'][i]['statistics'].get('favoriteCount'),
            vd_thumbnail = response['items'][i]['snippet']['thumbnails']['default']['url'],
            vd_caption_status = response['items'][i]['contentDetails'].get('caption'),
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

# Function to display YouTube channel information
def display_channel_info(channel_info):
    st.markdown("## Channel Information")
    st.write(f"**Channel ID:** {channel_info['ch_id']}")
    st.write(f"**Title:** {channel_info['ch_title']}")
    st.write(f"**Description:** {channel_info['description']}")
    st.write(f"**Published At:** {channel_info['publish_at']}")
    st.write(f"**Subscriber Count:** {channel_info['sb_count']}")
    st.write(f"**Video Count:** {channel_info['vd_count']}")
    st.write(f"**View Count:** {channel_info['view_count']}")
    st.write(f"**Playlist ID:** {channel_info['paylist_id']}")

# FUNCTION TO GET CHANNEL NAMES FROM MONGODB
def Get_channel_names():
    ch_names = []
    for i in database.Channel_Information.find():
        ch_names.append(i['channel_information']['ch_title'])
    return ch_names

#function to convert the duration to HH:MM:SS
def convert_duration(duration):
    # Use regular expression to extract hours, minutes, and seconds
    match = re.match(r'PT(\d+H)?(\d+M)?(\d+S)?', duration)
    if not match:
        return '00:00:00'
    # Extract groups and convert to integers, defaulting to 0 if not present
    hours = int(match.group(1)[:-1]) if match.group(1) else 0
    minutes = int(match.group(2)[:-1]) if match.group(2) else 0
    seconds = int(match.group(3)[:-1]) if match.group(3) else 0
    # Create a timedelta object with the extracted hours, minutes, and seconds
    duration_timedelta = timedelta(hours=hours, minutes=minutes, seconds=seconds)
    # Format the result as "hh:mm:ss"
    formatted_duration = str(duration_timedelta)
    return formatted_duration

#Function to create and Insert the channel details to mysql DB
def Insert_channel_details(selected_name):
    ch_list = collection.find({"channel_information.ch_title": selected_name},{"_id":0,"playlist_information":0,"video_information":0,"comments_information":0})
    ch_df = pd.DataFrame(list(ch_list))

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

#Function to create and Insert the playlist details to mysql DB
def Insert_playlist_details(selected_name):
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

#Function to create and Insert the Video details to mysql DB
def Insert_video_details(selected_name):
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
                                                                    channel_title VARCHAR(255),
                                                                    vd_title VARCHAR(255),
                                                                    vd_publish_at DATETIME,
                                                                    vd_description TEXT,
                                                                    vd_view_count INT,
                                                                    vd_like_count INT,
                                                                    vd_dislike_count INT,
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
        vd_tags_str = ', '.join(map(str, row['vd_tags'])) if row['vd_tags'] is not None else None
        vd_insert_query = '''INSERT INTO video_data(vd_id, playlist_Id, channel_title, vd_title,vd_publish_at,vd_description,\
                                                        vd_view_count,vd_like_count,vd_dislike_count,vd_comment_count,vd_duration,\
                                                        vd_tags,vd_favorite_Count,vd_thumbnail,vd_caption_status)\
                                                        VALUES(%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s,%s,%s)'''
        values = (
                row['vd_id'],
                row['playlist_Id'],
                row['channel_title'],
                row['vd_title'],
                row['vd_publish_at'],
                row['vd_description'],
                row['vd_view_count'],
                row['vd_like_count'],
                row['vd_dislike_count'],
                row['vd_comment_count'],
                convert_duration(row['vd_duration']),
                vd_tags_str,
                row['vd_favorite_Count'],
                row['vd_thumbnail'],
                row['vd_caption_status']
            )
        mycursor.execute(vd_insert_query,values)
        mydb.commit()

#Function to create and Insert the Comment details to mysql DB
def Insert_comment_details(selected_name):
    cm_list = collection.find_one({"channel_information.ch_title": selected_name},{"_id":0,"channel_information":0,"playlist_information":0,"video_information":0})
    new_cm_list = []
    for i in range(len(cm_list['comments_information'])):
        new_cm_list.append(cm_list['comments_information'][i])
    cm_df = pd.DataFrame(list(new_cm_list))
    # Drop Table If Exisits 
    # drop_query = '''DROP TABLE If EXISTS Comment_Details'''
    # mycursor.execute(drop_query)
    # mydb.commit()
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

# SQL Query Analysis
def ques_1():
    query_1 = '''SELECT channel_details.ch_title, video_data.vd_title 
                FROM channel_details
                LEFT JOIN playlist_details ON channel_details.ch_id = playlist_details.channel_id
                LEFT JOIN video_data ON playlist_details.play_list_id = video_data.playlist_Id'''
    mycursor.execute(query_1)
    sql_ans1 = mycursor.fetchall()
    df1 = pd.DataFrame(sql_ans1, columns=['channel name','video name'])
    # Set a custom index with a name
    df1.index = pd.RangeIndex(start=1, stop=len(df1) + 1, name='S.No')
    return df1
def ques_2():
    query_2 = '''SELECT channel_details.ch_title, channel_details.vd_count FROM channel_details ORDER BY vd_count DESC'''
    mycursor.execute(query_2)
    sql_ans2 = mycursor.fetchall()
    df2 = pd.DataFrame(sql_ans2, columns=['Channel Name','Video Count'])
    # Set a custom index with a name
    df2.index = pd.RangeIndex(start=1, stop=len(df2) + 1, name='S.No')
    return df2
def ques_3():
    query_3 = '''SELECT channel_details.ch_title, video_data.vd_view_count,video_data.vd_title 
                FROM channel_details
                LEFT JOIN playlist_details ON channel_details.ch_id = playlist_details.channel_id
                LEFT JOIN video_data ON playlist_details.play_list_id = video_data.playlist_Id ORDER BY video_data.vd_view_count DESC LIMIT 10'''
    mycursor.execute(query_3)
    sql_ans3 = mycursor.fetchall()
    df3 = pd.DataFrame(sql_ans3, columns=['Channel Name','View Count','Video Name'])
    # Set a custom index with a name
    df3.index = pd.RangeIndex(start=1, stop=len(df3) + 1, name='S.No')
    return df3
def ques_4():
    query_4 = '''SELECT video_data.vd_comment_count,video_data.vd_title FROM video_data ORDER BY vd_comment_count DESC'''
    mycursor.execute(query_4)
    sql_ans4 = mycursor.fetchall()
    df4 = pd.DataFrame(sql_ans4, columns=['Comment Count','Video Name'])
    # Set a custom index with a name
    df4.index = pd.RangeIndex(start=1, stop=len(df4) + 1, name='S.No')
    return df4
def ques_5():
    query_5 = '''SELECT video_data.vd_like_count,video_data.vd_title,video_data.channel_title FROM video_data ORDER BY vd_like_count DESC'''
    mycursor.execute(query_5)
    sql_ans5 = mycursor.fetchall()
    df5 = pd.DataFrame(sql_ans5, columns=['Like Count','Video Title','Channel Name'])
    # Set a custom index with a name
    df5.index = pd.RangeIndex(start=1, stop=len(df5) + 1, name='S.No')
    return df5
def ques_6():
    query_6 = '''SELECT video_data.vd_like_count,video_data.vd_dislike_count,video_data.vd_title FROM video_data ORDER BY vd_like_count DESC'''
    mycursor.execute(query_6)
    sql_ans6 = mycursor.fetchall()
    df6 = pd.DataFrame(sql_ans6, columns=['Like Count','Dislike Count','Video Title'])
    # Set a custom index with a name
    df6.index = pd.RangeIndex(start=1, stop=len(df6) + 1, name='S.No')
    return df6
def ques_7():
    query_7 = '''SELECT channel_details.ch_title,channel_details.view_count FROM channel_details ORDER BY view_count DESC'''
    mycursor.execute(query_7)
    sql_ans7 = mycursor.fetchall()
    df7 = pd.DataFrame(sql_ans7, columns=['Channel Name','View Count'])
    # Set a custom index with a name
    df7.index = pd.RangeIndex(start=1, stop=len(df7) + 1, name='S.No')
    return df7
def ques_8():
    query_8 = '''SELECT channel_details.ch_title,channel_details.publish_at FROM channel_details WHERE publish_at = "2022"'''
    mycursor.execute(query_8)
    sql_ans8 = mycursor.fetchall()
    df8 = pd.DataFrame(sql_ans8, columns=['Channel Name','Published Year'])
    # Set a custom index with a name
    df8.index = pd.RangeIndex(start=1, stop=len(df8) + 1, name='S.No')
    if df8.empty:
        return "No Channels are published at the year 2022"
    else:
        return df8
def ques_9():
    query_9 = '''SELECT
                    channel_details.ch_title,TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(video_data.vd_duration))),'%H:%i:%s')\
                    AS avg_duration FROM channel_details
                    LEFT JOIN playlist_details ON channel_details.ch_id = playlist_details.channel_id
                    LEFT JOIN video_data ON playlist_details.play_list_id = video_data.playlist_Id GROUP BY channel_details.ch_title
                '''
    mycursor.execute(query_9)
    sql_ans9 = mycursor.fetchall()
    df9 = pd.DataFrame(sql_ans9, columns=['Channel Name','Avg Duration'])
    # Set a custom index with a name
    df9.index = pd.RangeIndex(start=1, stop=len(df9) + 1, name='S.No')
    return df9
def ques_10():
    query_10 = '''SELECT channel_details.ch_title,video_data.vd_comment_count,video_data.vd_title FROM channel_details\
                 LEFT JOIN playlist_details ON channel_details.ch_id = playlist_details.channel_id\
                 LEFT JOIN video_data ON playlist_details.play_list_id = video_data.playlist_Id ORDER BY vd_comment_count DESC'''
    mycursor.execute(query_10)
    sql_ans10 = mycursor.fetchall()
    df10 = pd.DataFrame(sql_ans10, columns=['Channel Name','Comment Count','Video Title'])
    # Set a custom index with a name
    df10.index = pd.RangeIndex(start=1, stop=len(df10) + 1, name='S.No')
    return df10

# Streamlit App
def main():
    st.set_page_config(layout="wide")
    st.title("Youtube Data Harvesting and Warehousing App")
    with st.sidebar:
        menu = option_menu("App Menu", ["Extract & Upload to MongoDB", 'Migrate to MySQL', 'SQL Query Analysis'],icons=['cloud-upload', 'bi-table','bi-patch-question'], menu_icon="cast", default_index=0)
    # Sidebar menu
    # menu = st.sidebar.selectbox("Menu", ["Extract & Upload to MongoDB", "Migrate to MySQL", "SQL Query Analysis"])
    if menu == "Extract & Upload to MongoDB":
        st.subheader("Extract & Upload to MongoDB")
        # Input for Channel ID
        channel_id = st.text_input("Enter The Channel ID", placeholder="Channel ID")
        col1, col2 = st.columns([.3,1])
        with col1:
            extract_btn = st.button("Extract Channel Information")
        with col2:
            upload_btn = st.button("Upload To MongoDB")
        # Button to extract channel information
        if extract_btn:
            if channel_id:
                channel_info = get_channel_details(channel_id)
                display_channel_info(channel_info)
            else:
                st.warning("Please enter a Channel ID.")
        
        # Button to Upload Youtube Info to MongoDB
        if upload_btn:
            if channel_id:
                Insert = Get_Channels_Infos(channel_id)
                st.success(Insert)
            else:
                st.warning("Please select a Channel Name.")
    elif menu == "Migrate to MySQL":
        st.subheader("Migrate to MySQL")
        # Dropdown for selecting a channel name
        channel_names = Get_channel_names()
        selected_name = st.selectbox('Select Channel Name', options=channel_names, index=None, placeholder="Select Name...")
        # Button to migrate data to MySQL
        if st.button("Migrate to MySQL"):
            if selected_name:
                try:
                    Insert_channel_details(selected_name)
                    Insert_playlist_details(selected_name)
                    Insert_video_details(selected_name)
                    Insert_comment_details(selected_name)
                    st.success("Migration to MySQL Successful!")
                except Exception as e:
                    st.error(f"Error during migration: {e}")
            else:
                st.warning("Please select a Channel Name.")
    elif menu == "SQL Query Analysis":
        st.subheader("SQL Query Analysis")
        sql_question = ['1. What are the names of all the videos and their corresponding channels?',
                        '2. Which channels have the most number of videos and how many videos do they have?',
                        '3. What are the top 10 most viewed videos and their respective channels?',
                        '4. How many comments were made on each video, and what are their corresponding video names?',
                        '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                        '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                        '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                        '8. What are the names of all the channels that have published videos in the year 2022?',
                        '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                        '10. Which videos have the highest number of comments, and what are their corresponding channel names?']

        # Dropdown for selecting a SQL query
        select_question = st.selectbox('Select your Question', options=sql_question, index=None, placeholder="Select Question...")

        # Button to execute selected SQL query
        if st.button("Run Query"):
            if select_question == '1. What are the names of all the videos and their corresponding channels?':
                st.write(ques_1())
            elif select_question == '2. Which channels have the most number of videos and how many videos do they have?':
                st.write(ques_2())
            elif select_question == '3. What are the top 10 most viewed videos and their respective channels?':
                st.write(ques_3())
            elif select_question == '4. How many comments were made on each video, and what are their corresponding video names?':
                st.write(ques_4())
            elif select_question == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
                st.write(ques_5())
            elif select_question == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
                st.write(ques_6())
            elif select_question == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
                st.write(ques_7())
            elif select_question == '8. What are the names of all the channels that have published videos in the year 2022?':
                st.write(ques_8())
            elif select_question == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
                st.write(ques_9())
            elif select_question == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
                st.write(ques_10())

# Call the main function at the end
if __name__ == "__main__":
    main()
