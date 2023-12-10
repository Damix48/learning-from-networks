from typing import List, Tuple
import db
import datetime


def create_dataset(path: str, name: str, years: List[Tuple[datetime.datetime, datetime.datetime, str]]):
    for start_date, end_date, part in years:
        movie_actor_collaboration = db.get_movie_actor_collaboration(
            start_date=start_date, end_date=end_date)

        actors_collaboration = {}

        for movie_id, actor_id_1, actor_id_2 in movie_actor_collaboration:
            if (actor_id_1, actor_id_2) not in actors_collaboration:
                actors_collaboration[actor_id_1, actor_id_2] = 1
            else:
                actors_collaboration[actor_id_1, actor_id_2] += 1

        with open(f"{path}/{name}_{part}.csv", 'w') as file:
            file.write("actor_1_id,actor_2_id,counter\n")
            for (actor_id_1, actor_id_2), counter in actors_collaboration.items():
                file.write(f"{actor_id_1},{actor_id_2},{counter}\n")


# print(
#     f"There are {create_dataset(years='2008*2010,2012,2010-2012')} movie_actor_collaboration")
