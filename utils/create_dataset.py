from typing import List, Tuple
import db
import pandas as pd


def create_movies_dataset(path: str):
    genres = db.get_genres()
    movies = db.get_movies()

    genres_dict = {}

    genres_dict = {genre[0]: index for genre, index in zip(genres, range(len(genres)))}

    for i in range(len(movies)):
        movie = movies[i]
        m = list(movie)
        genres = movie[8]
        # create bag of words for genres
        genres_bag = [0] * len(genres_dict)
        for genre in genres:
            genres_bag[genres_dict[genre]] = 1
        m[8] = genres_bag
        movies[i] = tuple(m)

    movies_df = pd.DataFrame(
        movies,
        columns=[
            "id",
            "title",
            "release_date",
            "budget",
            "revenue",
            "vote_average",
            "vote_count",
            "popularity",
            "genres",
            "keywords",
        ],
    )

    movies_df.to_csv(f"{path}/movies.csv", index=False)


def create_people_dataset(path: str):
    person_types = ["actors", "directors", "writers"]

    for person_type in person_types:
        people = db.get_people(person_type)

        people_df = pd.DataFrame(
            people,
            columns=["id", "name", "popularity"],
        )

        name = f"{person_type}.csv"

        people_df.to_csv(f"{path}/{name}", index=False)


def create_movie_connections_dataset(path: str):
    person_types = ["actor", "director", "writer"]

    for person_type in person_types:
        people = db.get_movie_people(person_type)

        people_df = pd.DataFrame(
            people,
            columns=["movie_id", f"{person_type}_id"],
        )

        name = f"movie_{person_type}.csv"

        people_df.to_csv(f"{path}/{name}", index=False)


def create_people_connections_dataset(path: str):
    person_types = ["actor", "director", "writer"]

    for i in range(len(person_types)):
        for j in range(i, len(person_types)):
            people = db.get_people_connections([person_types[i], person_types[j]])

            people_df = pd.DataFrame(
                people,
                columns=[f"{person_types[i]}_id", f"{person_types[j]}_id"],
            )

            name = f"{person_types[i]}_{person_types[j]}.csv"

            people_df.to_csv(f"{path}/{name}", index=False)
