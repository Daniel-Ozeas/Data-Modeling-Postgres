import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def process_song_file(cur, filepath):
    """The process_song_file() get data from song_data folder to be insert in song_table and artist_table
    Parameters:
    cur: The cursor to execute sql commands
    filepath: The path where the song data files are;
    
    df: Open song file
    song_data: Select only data in columns song_id, title, artist_id, year, duration and insert in song_table
    artist_data: Select only data in columns artist_id, artist_name, artist_location, artist_latitude and   artist_longitude ant insert into artist_table.
    """

    #open song file
    df = pd.read_json(filepath, lines=True)

    #insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].dropna(axis=0, subset=['song_id'])
    song_data = song_data.drop_duplicates(['song_id'])
    song_data = song_data.values[0].tolist()

    
    cur.execute(song_table_insert, song_data)

    #insert artist record
    artist_data = df[["artist_id", "artist_name", "artist_location", "artist_latitude","artist_longitude"]]
    artist_data = artist_data.drop_duplicates(['artist_id'])
    artist_data = artist_data.values[0].tolist()
    
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """ The function get data from log_data paste, filter only those rows that contain NextSong in page column,\
    transform timestamp into separate data to be input in time_table, get the data about users and put into\
    users_table
    
    Parameters:
    cur: The cursor to execute sql commands
    filepath: The path where the song data files are
    
    df: read .json format files passed in the filepath, filter by 'NextSong' in page column and convert all \
    timestamp to datetaime
    
    t: receive the datetime column
    
    time_data: select and separate all time as hour, day, week_of_the_year, month, year, weekday and insert into\
    time_table;
    
    user_df: Select all data from log file and insert the coluns userId, firstName, lastName, gender and level\
    into user_table
    
    songplay_data: Join all data in song_table and artist_table from song, artist and length columns and select artist_id and song_id to be\
    insert in songplay_table with other columns like ts, userId, level, sessionId, location, userAgent
    
    """
    
    df = pd.read_json(filepath, lines=True)

    df = df[df['page'] == 'NextSong']

    df['ts'] = pd.to_datetime(df['ts'], unit ='ms')
    t = df['ts']

    
    time_data = pd.DataFrame({'timestamp': t, 'hour': t.dt.hour, 'day': t.dt.day, 'week_of_the_year': t.dt.dayofweek, 'month': t.dt.month,'year': t.dt.year, 'weekday': t.dt.weekday})
    column_labels = ('timestamp', 'hour', 'day', 'week_of_the_year', 'month', 'year', 'weekday' )
    time_df = time_data

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    user_df = df.loc[:,['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates(['userId'])

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
        
    for index, row in df.iterrows():

        
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        
        songplay_data = (row.ts, row.userId, row.level, \
                                     songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Parameters:
    cur: cursor to execute sql commands
    conn: to do the connection with database
    filepath: the path from the file
    func: function to be used
    
    Get all files matching extension from directory and save in all_files
    
    num_files: get total number of files found 
    
    Iteration over files and process
    
    """
    

    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()