import datetime
import time
from typing import List, Union, Tuple
import api
import db
import concurrent.futures
import os
from dotenv import load_dotenv

from api import WikipediaApi, TMDBApi
import pandas as pd
import re
import json

load_dotenv()

max_workers = int(os.getenv("MAX_WORKERS")) | 30


def populate_db(api_key: str, reset=False):
    if reset:
        db.reset_db()

    movies = pd.read_csv("data/wiki_movies.csv", dtype=str).values.tolist()

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(create_movie, [api_key] * len(movies), movies)

    movies = db.get_movies_ids()

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(update_movie, [api_key] * len(movies), movies)

    # Create people connections
    actors = db.get_people("actors")
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(create_people_connections, actors, ["actor"] * len(actors))

    directors = db.get_people("directors")
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(
            create_people_connections, directors, ["director"] * len(directors)
        )

    writers = db.get_people("writers")
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(create_people_connections, writers, ["writer"] * len(writers))


def create_movie_list(path: str = "data/wiki_movies.csv"):
    wikipedia_api = WikipediaApi()

    pages = wikipedia_api.get_all_category_members(
        "Category:2010s_English-language_films"
    )
    pages += wikipedia_api.get_all_category_members(
        "Category:2020s_English-language_films"
    )

    movies = pd.DataFrame(columns=["title", "year"])

    re_film = re.compile(r" \(.*?film\)$")
    re_year = re.compile(r" \((\d{4}) film\)$")
    for page in pages:
        title = page["title"]
        year = re_year.search(title).group(1) if re_year.search(title) else None
        title = re_film.sub("", page["title"])

        movies = movies._append({"title": title, "year": year}, ignore_index=True)

    movies.to_csv(path, index=False)


def create_movie(api_key: str, movie):
    tmdb_api = TMDBApi(api_key)

    title, year = movie
    tmdb_movie = tmdb_api.search_movie(title, year)
    if tmdb_movie is None:
        return

    db.create_movie(tmdb_movie["id"])


def update_movie(api_key: str, movie_id):
    movie_id = movie_id[0]

    tmdb_api = TMDBApi(api_key)

    tmdb_movie = tmdb_api.get_movie_details(movie_id)

    if tmdb_movie is None:
        return

    if tmdb_movie["vote_count"] < 1:
        return

    genres = [genre["id"] for genre in tmdb_movie["genres"]]
    keywords = [keyword["id"] for keyword in tmdb_movie["keywords"]["keywords"]]

    if len(genres) == 0:
        return

    db.update_movie(
        movie_id,
        tmdb_movie["title"],
        tmdb_movie["release_date"],
        tmdb_movie["budget"],
        tmdb_movie["revenue"],
        tmdb_movie["vote_average"],
        tmdb_movie["vote_count"],
        tmdb_movie["popularity"],
        genres,
        keywords,
    )

    create_people(
        movie_id, tmdb_movie["credits"]["cast"], tmdb_movie["credits"]["crew"]
    )


def create_people(movie, cast, crew):
    actors = []
    directors = []
    writers = []

    for i in range(min(len(cast), 10)):
        actor = cast[i]
        actors.append((actor["id"], actor["name"], actor["popularity"], ""))

    for person in crew:
        if person["job"] == "Director":
            directors.append((person["id"], person["name"], person["popularity"], ""))
        elif person["job"] == "Writer" or person["job"] == "Screenplay":
            writers.append((person["id"], person["name"], person["popularity"], ""))

    db.insert_people(actors, "actors")
    db.insert_people(directors, "directors")
    db.insert_people(writers, "writers")

    interactions = []
    for actor in actors:
        interactions.append((movie, actor[0]))
    db.insert_movie_people(interactions, "actor")

    interactions = []
    for director in directors:
        interactions.append((movie, director[0]))
    db.insert_movie_people(interactions, "director")

    interactions = []
    for writer in writers:
        interactions.append((movie, writer[0]))
    db.insert_movie_people(interactions, "writer")


def create_people_connections(person, person_type):
    person_id = person[0]
    person_name = person[1]

    wikipedia_api = WikipediaApi()

    links = wikipedia_api.get_all_links(person_name)

    types = [
        "actor",
        "director",
        "writer",
    ]

    if len(links) == 0:
        return

    start_index = types.index(person_type)

    for type_ in types[start_index:]:
        people = db.search_people(type_ + "s", links)

        if len(people) == 0:
            continue

        interactions = []
        for p in people:
            p = p[0]
            if type_ == person_type:
                interactions.append(sorted((person_id, p)))
            else:
                interactions.append((person_id, p))

        print(
            f"Found {len(interactions)} {type_}s for {person_name} to {[person_type, type_]}"
        )

        db.insert_people_connections(interactions, [person_type, type_])


def update_people(api_key: str, person_id):
    tmdb_api = TMDBApi(api_key)

    tmdb_person = tmdb_api.get_person_details(person_id)

    if tmdb_person is None:
        return

    db.update_person(
        person_id,
        tmdb_person["name"],
        tmdb_person["popularity"],
        tmdb_person["biography"],
        tmdb_person["birthday"],
        tmdb_person["deathday"],
    )
