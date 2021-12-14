from multiprocessing import Pool, cpu_count
from types import FunctionType
from typing import List, Optional

from pandas.core.frame import DataFrame
from musixmatch_api import Musixmatch
import os
import json
import pandas as pd
import traceback
import logging
import csv

logger = logging.getLogger(__name__)

# Create handlers
f_handler = logging.FileHandler(f'{__file__}.log')
f_handler.setLevel(logging.WARNING)
# Create formatters and add it to handlers
f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
f_handler.setFormatter(f_format)
# Add handlers to the logger
logger.addHandler(f_handler)

API_KEY = "cf792dbf5a2b8f87475710b6238920a5"
BASE_PATH = os.path.dirname(__file__)


def add_genre_id_to_track(track: dict):
    primary_genres = track.pop("primary_genres")
    music_genre_id = -1
    try:
        music_genre_id = primary_genres.get("music_genre_list")[0].get("music_genre").get("music_genre_id")
    except:
        logger.error(f"track: {track.get('track_id')} has not primary_genres\n{track}")
        logger.exception("")
    track["music_genre_id"] = music_genre_id
    return track

def line2list(line: str) -> list:
    line.rstrip("\n")
    array = json.loads(line)
    return array
    

def main():
    music = Musixmatch(API_KEY, verbose=True)
    src_dir = os.path.join(BASE_PATH, "src_ru")
    genres_json_path = os.path.join(src_dir, 'genres.json')
    genres_tsv_path = os.path.join(src_dir, 'genres.tsv')

    music_genres_list = music.get_genres()
    print(music_genres_list)
    exit()
    genres_df = pd.DataFrame(music_genres_list)
    genres_df.to_csv(genres_tsv_path, sep='\t', index=False)

    genres_df = pd.read_csv(genres_tsv_path, sep='\t')
    music_genre_ids = genres_df['music_genre_id']
    tracks_file_path = os.path.join(src_dir, 'tracks.json')
    tracks_tsv_path = os.path.join(src_dir, 'tracks.tsv')

    music.download_all_tracks(tracks_file_path, music_genre_ids)
    with open(tracks_file_path, "r") as file:
        track_list = []
        with Pool(cpu_count()) as pool:
            for tracks in pool.imap(line2list, file.readlines()):
                track_list.extend(tracks)
    tracks_df = pd.DataFrame(track_list)
    tracks_df.to_csv(tracks_tsv_path, sep="\t", index=False)

    # all_tracks = pd.read_csv(tracks_tsv_path, sep='\t')
    all_tracks = tracks_df

    all_tracks.sort_values("track_id", inplace=True)
    all_tracks.drop_duplicates(subset='track_id', inplace=True)
    print("Count trcks: ", all_tracks.shape)
    un_tracks_tsv_path = os.path.join(src_dir, 'unique_tracks.tsv')
    all_tracks.to_csv(un_tracks_tsv_path, sep="\t", index=False)
    # all_tracks = pd.read_csv(un_tracks_tsv_path, sep='\t')
    has_lyrics = all_tracks.loc[all_tracks['has_lyrics'] == 1]
    # has_lyrics = has_lyrics.loc[has_lyrics["track_id"].isin(has_lyrics['track_id'].to_list()[:10])]
    print("Tracks with lyrics", has_lyrics.shape)
    lyrics_file_path = os.path.join(src_dir, "lyrics.tsv")
    print("Download lyrics")
    ids = has_lyrics['track_id'].to_list()
    result = music.download_lyrics(ids)
    with open("src/lyrics.json", "w") as file:
        json.dump(result, file)
    df = pd.DataFrame(result)
    df.to_csv(lyrics_file_path, "\t")
    # lyrics = pd.read_csv(lyrics_file_path, sep='\t')
    print(df)
    

    


    

if __name__ == "__main__":
    main()