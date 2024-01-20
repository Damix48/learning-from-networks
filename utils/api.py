import datetime
import os
import requests
from typing import List, Union
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("API_KEY")
base_url = "https://api.themoviedb.org/3"

headers = {"accept": "application/json", "Authorization": f"Bearer {api_key}"}


def create_params(
    page: int = 1,
    genres: List[int] = None,
    from_date: Union[datetime.datetime, str] = None,
    to_date: Union[datetime.datetime, str] = None,
):
    return {
        "include_adult": False,
        "include_video": False,
        "language": "en-US",
        "page": page,
        "primary_release_date.gte": isinstance(from_date, datetime.datetime)
        and from_date.isoformat()
        or from_date,
        "primary_release_date.lte": isinstance(to_date, datetime.datetime)
        and to_date.isoformat()
        or to_date,
        "sort_by": "primary_release_date.asc",
        "with_genres": genres,
        "without_genres": "16 | 99 | 10770",
        "with_origin_country": "US",
    }


def get_movies_page_count(
    genres: List[int] = None,
    from_date: Union[datetime.datetime, str] = None,
    to_date: Union[datetime.datetime, str] = None,
):
    return requests.get(
        f"{base_url}/discover/movie",
        headers=headers,
        params=create_params(1, genres, from_date, to_date),
    ).json()["total_pages"]


def get_movies(
    page: int = 1,
    genres: List[int] = None,
    from_date: Union[datetime.datetime, str] = None,
    to_date: Union[datetime.datetime, str] = None,
):
    results = requests.get(
        f"{base_url}/discover/movie",
        headers=headers,
        params=create_params(page, genres, from_date, to_date),
    ).json()["results"]

    movies = [(movie["id"], movie["title"], movie["release_date"]) for movie in results]

    return movies


def get_actors(movie_id: int):
    results = requests.get(
        f"{base_url}/movie/{movie_id}/credits", headers=headers
    ).json()["cast"]

    actors = [
        (actor["id"], actor["name"], actor["popularity"])
        for actor in results
        if actor["known_for_department"] == "Acting" and actor["adult"] == False
    ]

    return actors
