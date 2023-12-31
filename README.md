# Youtube-Data-Harvesting-And-Migration-
Unlock the full potential of your YouTube data with our in-depth guide on data harvesting and migration! Whether you're a content creator, marketer, or business owner, understanding how to efficiently collect and transfer your YouTube data is essential for optimizing your online presence.
# Dependencies
streamlit
googleapiclient
pymongo
mysql-connector-python
pandas
re
datetime
# Usage
## 1. Streamlit Interface
Input the YouTube Channel ID.
Click the "Extract Here" button to fetch and display channel information.
Click the "Upload To MongoDB" button to upload channel information to MongoDB.
Select a channel name from the dropdown.
Click the "Migrate" button to migrate MongoDB data to MySQL.
## 2. Queries and Analysis
Explore the provided SQL queries for data analysis. Questions range from the most viewed videos to the average duration of videos for each channel.

## Code Structure
## YouTube Data Extraction:

get_channel_details: Fetches details about a YouTube channel.
get_playlist_details: Retrieves details about playlists associated with a channel.
Get_video_Ids: Retrieves video IDs from a playlist.
Get_video_details: Fetches details about videos, including comments.
get_comment_details: Retrieves details about comments on videos.
## Database Operations:

Insert_channel_details: Inserts channel details into the MySQL database.
Insert_playlist_details: Inserts playlist details into the MySQL database.
Insert_video_details: Inserts video details into the MySQL database.
Insert_comment_details: Inserts comment details into the MySQL database.
# Streamlit Interface:
Provides an interactive UI for extracting, uploading, and querying data.
# SQL Queries and Analysis:

Several SQL queries for analyzing data and answering specific questions.
# Database Schema
Channel_Details: Stores information about YouTube channels.
Playlist_Details: Contains details about playlists associated with channels.
Video_Data: Stores information about videos, including their duration and statistics.
Comment_Details: Contains details about comments on videos.
# Data Analysis
Explore various SQL queries to analyze and extract insights from the collected YouTube data.
# ScreenShots for this app
## Extract & Upload to MongoDB
![image](https://github.com/chandrika0918/Youtube-Data-Harvesting-And-Migration-/assets/143815211/1f80bc89-a016-4be8-b402-c5bffe52a42e)

## Migrate to MySQL
![image](https://github.com/chandrika0918/Youtube-Data-Harvesting-And-Migration-/assets/143815211/145dbd12-aab9-4fab-8115-7f932cf80b9d)

## SQL Query Analysis
![image](https://github.com/chandrika0918/Youtube-Data-Harvesting-And-Migration-/assets/143815211/d30a5fb2-e55a-42ce-b92a-4b24e1ab6673)

## Demo Video
### Click Here to view demo video
linkedin: https://www.linkedin.com/posts/vijayachandrika-r-bb554023a_datascience-youtube-data-activity-7150115235972939776-UGOq?utm_source=share&utm_medium=member_desktop

# Conclusion
This script facilitates the extraction, storage, and analysis of YouTube data, providing valuable insights into channel and video performance. Customize queries or add new ones to suit your analytical needs.
 
