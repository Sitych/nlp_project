import json
from typing import List, Optional
import requests
import urllib.parse
import logging
from multiprocessing import Pool, cpu_count
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import tqdm

from requests.api import get

class Musixmatch:

    __protocol = "https"
    __domain = "api.musixmatch.com"
    __path_part = "ws/1.1"
    logger = None
    

    def __init__(self, api_key, verbose: bool = False):
        self.logger = logging.getLogger(__name__)
        # Create handlers
        f_handler = logging.StreamHandler()
        f_handler.setLevel(logging.INFO)
        # Create formatters and add it to handlers
        f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        f_handler.setFormatter(f_format)
        # Add handlers to the logger
        self.logger.addHandler(f_handler)
        self.__logger_level = logging.DEBUG if verbose else logging.INFO
        self.logger.setLevel(self.__logger_level)
        self.__api_key = api_key

    def __set_logger_level(self, level: int):
        levels = [logging.DEBUG, logging.INFO,
                  logging.WARNING, logging.ERROR, logging.CRITICAL]
        if level not in levels:
            raise RuntimeError(
                f"level {level} does not exist. Choise from {levels}")

        self.__logger_level = level
        self.logger.setLevel(level)

    def create_api_query(self, api_method: str, query_args: dict) -> str:
        query_args["apikey"] = self.__api_key
        path = f"{self.__path_part}/{api_method}"
        query = "&".join(
            [f"{arg}={value}" for arg, value in query_args.items()])
        api_query = urllib.parse.urlunparse([
            self.__protocol,
            self.__domain,
            path,
            "",
            query,
            ""
        ])
        return api_query

    def get_request(self, api_method: str, query_args: dict, retries: int = 5) -> Optional[dict]:
        url = self.create_api_query(api_method, query_args)
        self.logger.debug(url)
        while retries:
            retries -= 1
            try:
                response = requests.get(url)
            except:
                self.logger.exception(f"Error with url: {url}")
                if retries == 0:
                    return {}
            if response.status_code != 200:
                self.logger.error(f"requests.get({url}) retrun status code {response.status_code}")
            else:
                break
        self.logger.debug(response.text)
        dict_response = json.loads(response.text)
        return dict_response

    def music_genres_get(self):
        api_method = "music.genres.get"
        query_args = {}
        return self.get_request(api_method, query_args)

    def track_lyrics_get(self, track_id, commontrack_id=None) -> dict:
        """
        Get the lyrics of a track.
        PARAMETERS
        track_id: The Musixmatch track id
        commontrack_id: The Musixmatch commontrack id
        https://developer.musixmatch.com/documentation/api-reference/track-lyrics-get
        """

        api_method = "track.lyrics.get"
        query_args = {"track_id": track_id}
        if commontrack_id:
            query_args["commontrack_id"] = commontrack_id

        return self.get_request(api_method, query_args)

    def track_search(self, page, page_size=100, f_has_lyrics: bool = True, **kwargs):
        """
        Search for track in Musixmatch database.
        PARAMETERS
        q_track: The song title
        q_artist: The song artist
        q_lyrics: Any word in the lyrics
        q_track_artist: Any word in the song title or artist name
        q_writer: Search among writers
        q: Any word in the song title or artist name or lyrics
        f_artist_id: When set, filter by this artist id
        f_music_genre_id: When set, filter by this music category id
        f_lyrics_language: Filter by the lyrics language (en,it,..)
        f_has_lyrics: When set, filter only contents with lyrics
        f_track_release_group_first_release_date_min: When set, filter the tracks with release date newer than value, format is YYYYMMDD
        f_track_release_group_first_release_date_max: When set, filter the tracks with release date older than value, format is YYYYMMDD
        s_artist_rating: Sort by our popularity index for artists (asc|desc)
        s_track_rating: Sort by our popularity index for tracks (asc|desc)
        quorum_factor: Search only a part of the given query string.Allowed range is (0.1 – 0.9)
        page: Define the page number for paginated results
        page_size: Define the page size for paginated results. Range is 1 to 100.
        """

        api_method = "track.search"
        query_args = {
            "page": page,
            "page_size": page_size,
            "f_has_lyrics": f_has_lyrics,
        }
        query_args.update(kwargs)
        for key in query_args.keys():
            if query_args[key] is None:
                raise TypeError(f"Value {key} is None. Expected str")

        return self.get_request(api_method, query_args)

    def get_genres(self) -> List[dict]:
        response = self.music_genres_get()
        body = self.get_body_response(response)
        music_genre_list = body.pop('music_genre_list')
        genres = [genre.pop('music_genre') for genre in music_genre_list]
        return genres
    
    @staticmethod
    def _get_track(elem: dict, music_genre_id) -> dict:
        track = elem.pop("track", {})
        track.pop("primary_genres", None)
        track.pop("track_name_translation_list", None)
        track["music_genre_id"] = music_genre_id
        return track

    def download_all_tracks(self, path: str, music_genre_ids: list, language: str = "en") -> None:
        """Download all tracks for every music_genre_id

        Args:
            path (str): path to json file
            music_genre_ids (list): list of genre ids

        
        """
        page_indexes = dict()
        for music_genre_id in music_genre_ids:
            page_index = 0
            self.logger.info(f"Download tracks for genre id = {music_genre_id}")
            while True:
                try:
                    tracks = self.track_search(
                        page=page_index, music_genre_id=music_genre_id, f_lyrics_language=language,
                        )
                    page_index += 1

                    body = self.get_body_response(tracks)
                    mini_track_list = body.pop('track_list', None)
                    if mini_track_list is None:
                        self.logger.error("body must contain field track_list")
                        continue

                    count_tracks = len(mini_track_list)
                    self.logger.info(f"Get {count_tracks} tracks")
                    if count_tracks == 0:
                        break

                    with Pool(cpu_count()) as pool:
                        func = partial(self._get_track, music_genre_id=music_genre_id)
                        tracks = list(pool.imap(func, mini_track_list))

                    with open(path, "a") as track_file:
                        line = json.dumps(tracks)
                        track_file.write(f"{line}\n")
                    self.logger.debug("Append new tracks")

                except Exception:
                    self.logger.exception(f"{tracks}, {page_index}")
                    break
            page_indexes[music_genre_id] = page_index
        self.logger.info("Download all files")
        return page_indexes
    
    @staticmethod
    def get_body_response(response: dict) -> dict:
        body = {}
        try:
            message = response["message"]
            body = message['body']
        except KeyError:
            print(f"Response is empty. Response: {response}")

        return body
    
    def get_lyrics(self, track_id: str, retries: int = 10) -> Optional[dict]:
        lyrics = {}
        while retries:
            track_lyrics = self.track_lyrics_get(track_id)
            body = self.get_body_response(track_lyrics)
            try:
                lyrics = body["lyrics"]
                break
            except (KeyError, TypeError):
                self.logger.debug(f"Request with track_id = {track_id} has not lyrics. retries: {retries}")
                retries -= 1
        if lyrics == {}:
            self.logger.error(f"Empty request with track_id = {track_id}.")
        lyrics.pop("script_tracking_url", None)
        lyrics.pop("pixel_tracking_url", None)
        lyrics.pop("lyrics_copyright", None)
        lyrics.pop("updated_time", None)
        lyrics['track_id'] = track_id

        return lyrics
    
    def download_lyrics(self, track_ids: List[str], therds=2) -> List[dict]:
        with ThreadPoolExecutor(therds) as executor:
            result = list(tqdm.tqdm(executor.map(self.get_lyrics, track_ids), total=len(track_ids)))
        return result
