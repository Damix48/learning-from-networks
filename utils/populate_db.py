import datetime
import time
from typing import List, Union, Tuple
import api
import db
import concurrent.futures
import os
from dotenv import load_dotenv

load_dotenv()

max_workers = int(os.getenv("MAX_WORKERS")) | 30


def populate_db(years: List[Tuple[datetime.datetime, datetime.datetime, str]], reset=False):
    if reset:
        db.reset_db()

    genres = []

    for from_date, to_date, _ in years:
        total_pages = api.get_movies_page_count(
            genres=genres,
            from_date=from_date,
            to_date=to_date
        )

        pages = range(1, total_pages+1)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            executor.map(populate_movies,
                         pages,
                         [genres] * len(pages),
                         [from_date] * len(pages),
                         [to_date] * len(pages)
                         )

    movies_id = [movie[0] for movie in db.get_movies()]

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(populate_actors, movies_id)


def populate_movies(page: int = 1, genres: List[int] = None, from_date: Union[datetime.datetime, str] = None, to_date: Union[datetime.datetime, str] = None):
    movies = api.get_movies(page, genres, from_date, to_date)

    db.insert_movies(movies)


def populate_actors(movie_id: int):
    time.sleep(0.5)
    actors = api.get_actors(movie_id)

    movie_actor = [(movie_id, actor[0]) for actor in actors]

    movie_actor_collaboration = []
    for i in range(len(actors)):
        for j in range(i+1, len(actors)):
            movie_actor_collaboration.append(
                (movie_id, actors[i][0], actors[j][0]))

    actor_collaboration = []
    for i in range(len(actors)):
        for j in range(i+1, len(actors)):
            actor_collaboration.append(sorted((actors[i][0], actors[j][0])))

    db.insert_actors(actors)

    db.insert_movie_actor(movie_actor)

    db.insert_movie_actor_collaboration(movie_actor_collaboration)

    db.insert_actor_collaboration(actor_collaboration)
