from typing import List, Tuple
import db
import datetime


def create_actors_dataset(
    path: str, name: str, years: List[Tuple[datetime.datetime, datetime.datetime, str]]
):
    for start_date, end_date, part in years:
        actors_movies = db.get_actors_movies(
            start_date=start_date, end_date=end_date, min_popularity=30
        )

        with open(f"{path}/{name}_{part}.csv", "w", encoding="utf-8") as file:
            file.write("id,name,popularity,movie_counter\n")
            for (
                actor_id,
                actor_name,
                actor_popularity,
                movie_counter,
            ) in actors_movies:
                file.write(
                    f'{actor_id},"{actor_name}",{actor_popularity},{movie_counter}\n'
                )


def create_collaborations_dataset(
    path: str, name: str, years: List[Tuple[datetime.datetime, datetime.datetime, str]]
):
    for start_date, end_date, part in years:
        movie_actor_collaboration = db.get_movie_actor_collaboration(
            start_date=start_date, end_date=end_date, min_popularity=30
        )

        actors_collaboration = {}

        for movie_id, actor_id_1, actor_id_2 in movie_actor_collaboration:
            if (actor_id_1, actor_id_2) not in actors_collaboration:
                actors_collaboration[actor_id_1, actor_id_2] = 1
            else:
                actors_collaboration[actor_id_1, actor_id_2] += 1

        with open(f"{path}/{name}_{part}.csv", "w", encoding="utf-8") as file:
            file.write("actor_1_id,actor_2_id,counter\n")
            for (actor_id_1, actor_id_2), counter in actors_collaboration.items():
                file.write(f"{actor_id_1},{actor_id_2},{counter}\n")
