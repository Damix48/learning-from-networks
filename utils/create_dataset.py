from typing import List, Tuple
import db
import pandas as pd


def create_movies_dataset(path: str, name: str = "movies.csv"):
    genres = db.get_genres()
    movies = db.get_movies()

    genres_dict = {}

    genres_dict = {genre[0]: index for genre, index in zip(genres, range(len(genres)))}

    print(genres_dict)

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

    movies_df.to_csv(f"{path}/{name}", index=False)


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


create_movies_dataset("dataset")
create_people_dataset("dataset")
create_movie_connections_dataset("dataset")
create_people_connections_dataset("dataset")

# def create_actors_dataset(
#     path: str, name: str, years: List[Tuple[datetime.datetime, datetime.datetime, str]]
# ):
#     for start_date, end_date, part in years:
#         actors_movies = db.get_actors_movies(
#             start_date=start_date, end_date=end_date, min_popularity=30
#         )

#         with open(f"{path}/{name}_{part}.csv", "w", encoding="utf-8") as file:
#             file.write("id,name,popularity,movie_counter\n")
#             for (
#                 actor_id,
#                 actor_name,
#                 actor_popularity,
#                 movie_counter,
#             ) in actors_movies:
#                 file.write(
#                     f'{actor_id},"{actor_name}",{actor_popularity},{movie_counter}\n'
#                 )


# def create_collaborations_dataset(
#     path: str, name: str, years: List[Tuple[datetime.datetime, datetime.datetime, str]]
# ):
#     for start_date, end_date, part in years:
#         movie_actor_collaboration = db.get_movie_actor_collaboration(
#             start_date=start_date, end_date=end_date, min_popularity=30
#         )

#         actors_collaboration = {}

#         for movie_id, actor_id_1, actor_id_2 in movie_actor_collaboration:
#             if (actor_id_1, actor_id_2) not in actors_collaboration:
#                 actors_collaboration[actor_id_1, actor_id_2] = 1
#             else:
#                 actors_collaboration[actor_id_1, actor_id_2] += 1

#         with open(f"{path}/{name}_{part}.csv", "w", encoding="utf-8") as file:
#             file.write("actor_1_id,actor_2_id,counter\n")
#             for (actor_id_1, actor_id_2), counter in actors_collaboration.items():
#                 file.write(f"{actor_id_1},{actor_id_2},{counter}\n")
