import datetime
import os
from typing import List, Tuple, Union
import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv

load_dotenv()

db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")
max_connections = int(os.getenv("MAX_CONNECTIONS")) | 100

db_connections = pool.ThreadedConnectionPool(
    minconn=1,
    maxconn=max_connections,
    host=db_host,
    port=db_port,
    dbname=db_name,
    user=db_user,
    password=db_password,
)

sql_create_movies = """
-- Create the "movies" table
CREATE TABLE movies (
    id INTEGER PRIMARY KEY,
    title TEXT,
    release_date DATE,
    budget INTEGER,
    revenue INTEGER,
    vote_average REAL,
    vote_count INTEGER,
    popularity REAL,
    genres INTEGER[],
    keywords INTEGER[]
);
"""

sql_create_actors = """
-- Create the "actors" table
CREATE TABLE actors (
    id INTEGER PRIMARY KEY,
    name TEXT,
    popularity REAL,
    biography TEXT
);
"""

sql_create_directors = """
-- Create the "directors" table
CREATE TABLE directors (
    id INTEGER PRIMARY KEY,
    name TEXT,
    popularity REAL,
    biography TEXT
);
"""

sql_create_writers = """
-- Create the "writers" table
CREATE TABLE writers (
    id INTEGER PRIMARY KEY,
    name TEXT,
    popularity REAL,
    biography TEXT
);
"""

sql_create_movie_actor = """
-- Create the "movie_actor" table to represent the many-to-many relationship between movies and actors
CREATE TABLE movie_actor (
    movie_id INTEGER,
    actor_id INTEGER,
    FOREIGN KEY (movie_id) REFERENCES movies(id),
    FOREIGN KEY (actor_id) REFERENCES actors(id),
    PRIMARY KEY (movie_id, actor_id)
);
"""

sql_create_movie_director = """
-- Create the "movie_director" table to represent the many-to-many relationship between movies and directors
CREATE TABLE movie_director (
    movie_id INTEGER,
    director_id INTEGER,
    FOREIGN KEY (movie_id) REFERENCES movies(id),
    FOREIGN KEY (director_id) REFERENCES directors(id),
    PRIMARY KEY (movie_id, director_id)
);
"""

sql_create_movie_writer = """
-- Create the "movie_writer" table to represent the many-to-many relationship between movies and writers
CREATE TABLE movie_writer (
    movie_id INTEGER,
    writer_id INTEGER,
    FOREIGN KEY (movie_id) REFERENCES movies(id),
    FOREIGN KEY (writer_id) REFERENCES writers(id),
    PRIMARY KEY (movie_id, writer_id)
);
"""

sql_create_actor_actor = """
-- Create the "actor_actor" table to represent the many-to-many relationship between movies and actors
CREATE TABLE actor_actor (
    actor_1_id INTEGER,
    actor_2_id INTEGER,
    FOREIGN KEY (actor_1_id) REFERENCES actors(id),
    FOREIGN KEY (actor_2_id) REFERENCES actors(id),
    PRIMARY KEY (actor_1_id, actor_2_id)
);
"""

sql_create_actor_director = """
-- Create the "actor_director" table to represent the many-to-many relationship between movies and actors
CREATE TABLE actor_director (
    actor_1_id INTEGER,
    director_2_id INTEGER,
    FOREIGN KEY (actor_1_id) REFERENCES actors(id),
    FOREIGN KEY (director_2_id) REFERENCES directors(id),
    PRIMARY KEY (actor_1_id, director_2_id)
);
"""

sql_create_actor_writer = """
-- Create the "actor_writer" table to represent the many-to-many relationship between movies and actors
CREATE TABLE actor_writer (
    actor_1_id INTEGER,
    writer_2_id INTEGER,
    FOREIGN KEY (actor_1_id) REFERENCES actors(id),
    FOREIGN KEY (writer_2_id) REFERENCES writers(id),
    PRIMARY KEY (actor_1_id, writer_2_id)
);
"""

sql_create_director_director = """
-- Create the "director_director" table to represent the many-to-many relationship between movies and actors
CREATE TABLE director_director (
    director_1_id INTEGER,
    director_2_id INTEGER,
    FOREIGN KEY (director_1_id) REFERENCES directors(id),
    FOREIGN KEY (director_2_id) REFERENCES directors(id),
    PRIMARY KEY (director_1_id, director_2_id)
);
"""

sql_create_director_writer = """
-- Create the "director_writer" table to represent the many-to-many relationship between movies and actors
CREATE TABLE director_writer (
    director_1_id INTEGER,
    writer_2_id INTEGER,
    FOREIGN KEY (director_1_id) REFERENCES directors(id),
    FOREIGN KEY (writer_2_id) REFERENCES writers(id),
    PRIMARY KEY (director_1_id, writer_2_id)
);
"""

sql_create_writer_writer = """
-- Create the "writer_writer" table to represent the many-to-many relationship between movies and actors
CREATE TABLE writer_writer (
    writer_1_id INTEGER,
    writer_2_id INTEGER,
    FOREIGN KEY (writer_1_id) REFERENCES writers(id),
    FOREIGN KEY (writer_2_id) REFERENCES writers(id),
    PRIMARY KEY (writer_1_id, writer_2_id)
);
"""


def reset_db():
    try:
        delete_db()
    except psycopg2.errors.UndefinedTable:
        pass

    create_db()


def create_db():
    conn = psycopg2.connect(
        dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port
    )
    cur = conn.cursor()

    cur.execute(sql_create_movies)
    cur.execute(sql_create_actors)
    cur.execute(sql_create_directors)
    cur.execute(sql_create_writers)
    cur.execute(sql_create_movie_actor)
    cur.execute(sql_create_movie_director)
    cur.execute(sql_create_movie_writer)
    cur.execute(sql_create_actor_actor)
    cur.execute(sql_create_actor_director)
    cur.execute(sql_create_actor_writer)
    cur.execute(sql_create_director_director)
    cur.execute(sql_create_director_writer)
    cur.execute(sql_create_writer_writer)

    conn.commit()

    cur.close()
    conn.close()


def delete_db():
    conn = psycopg2.connect(
        dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port
    )
    cur = conn.cursor()

    cur.execute("DROP TABLE actor_writer")
    cur.execute("DROP TABLE actor_director")
    cur.execute("DROP TABLE actor_actor")
    cur.execute("DROP TABLE writer_writer")
    cur.execute("DROP TABLE director_writer")
    cur.execute("DROP TABLE director_director")
    cur.execute("DROP TABLE movie_writer")
    cur.execute("DROP TABLE movie_director")
    cur.execute("DROP TABLE movie_actor")
    cur.execute("DROP TABLE writers")
    cur.execute("DROP TABLE directors")
    cur.execute("DROP TABLE movies")
    cur.execute("DROP TABLE actors")

    conn.commit()

    cur.close()
    conn.close()


def create_movie(movie_id: int):
    conn = db_connections.getconn()
    cur = conn.cursor()

    cur.execute(
        "INSERT INTO movies VALUES (%s) ON CONFLICT (id) DO NOTHING", (movie_id,)
    )

    conn.commit()

    cur.close()
    db_connections.putconn(conn)


def update_movie(
    movie_id: int,
    title: str,
    release_date: datetime.date,
    budget: int,
    revenue: int,
    vote_average: float,
    vote_count: int,
    popularity: float,
    genres: List[int],
    keywords: List[int],
):
    conn = db_connections.getconn()
    cur = conn.cursor()

    sql_update_movie = """
    UPDATE
        movies
    SET
        title = %s,
        release_date = %s,
        budget = %s,
        revenue = %s,
        vote_average = %s,
        vote_count = %s,
        popularity = %s,
        genres = %s,
        keywords = %s
    WHERE
        id = %s;
    """

    cur.execute(
        sql_update_movie,
        (
            title,
            release_date,
            budget,
            revenue,
            vote_average,
            vote_count,
            popularity,
            genres,
            keywords,
            movie_id,
        ),
    )

    conn.commit()

    cur.close()
    db_connections.putconn(conn)


def insert_people(actors: List[Tuple[int, str, float, str]], people_type: str):
    print(f"Inserting {len(actors)} {people_type}")

    conn = db_connections.getconn()
    cur = conn.cursor()

    cur.executemany(
        f"INSERT INTO {people_type} VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO NOTHING",
        actors,
    )

    conn.commit()

    cur.close()
    db_connections.putconn(conn)


def update_people(
    people_id,
    people_type: str,
    people_name=None,
    people_popularity=None,
    people_biography=None,
):
    conn = db_connections.getconn()
    cur = conn.cursor()

    sql_update_people = f"""
    UPDATE
        {people_type}
    SET
        name = COALESCE(%s, name),
        popularity = COALESCE(%s, popularity),
        biography = COALESCE(%s, biography)
    WHERE
        id = %s;
    """

    cur.execute(
        sql_update_people,
        (
            people_name,
            people_popularity,
            people_biography,
            people_id,
        ),
    )

    conn.commit()

    cur.close()
    db_connections.putconn(conn)


def insert_movie_people(movie_actor: List[Tuple[int, int]], people_type: str):
    print(f"Inserting {len(movie_actor)} movie_{people_type}")

    conn = db_connections.getconn()
    cur = conn.cursor()

    cur.executemany(f"INSERT INTO movie_{people_type} VALUES (%s, %s)", movie_actor)

    conn.commit()

    cur.close()
    db_connections.putconn(conn)


def insert_people_connections(
    people_connections: List[Tuple[int, int]], people_type: List[str]
):
    conn = db_connections.getconn()
    cur = conn.cursor()

    sql_people_connections = f"""
    INSERT INTO
        {people_type[0]}_{people_type[1]}
    VALUES
        (%s, %s)
    ON CONFLICT ({people_type[0]}_1_id, {people_type[1]}_2_id) DO NOTHING;
    """

    cur.executemany(
        sql_people_connections,
        people_connections,
    )

    conn.commit()

    cur.close()
    db_connections.putconn(conn)


def get_movies_ids():
    conn = db_connections.getconn()
    cur = conn.cursor()

    cur.execute("SELECT id FROM movies;")

    movies = cur.fetchall()

    cur.close()
    db_connections.putconn(conn)

    return movies


def get_people_ids(people_type: str):
    conn = db_connections.getconn()
    cur = conn.cursor()

    cur.execute(f"SELECT id FROM {people_type};")

    people = cur.fetchall()

    cur.close()
    db_connections.putconn(conn)

    return people


def get_people(people_type: str):
    conn = db_connections.getconn()
    cur = conn.cursor()

    cur.execute(f"SELECT * FROM {people_type};")

    people = cur.fetchall()

    cur.close()
    db_connections.putconn(conn)

    return people


def search_people(people_type: str, people_names: List[str]):
    conn = db_connections.getconn()
    cur = conn.cursor()

    sql_search_people = f"""
    SELECT
        id
    FROM
        {people_type}
    WHERE
        name IN %s;
    """

    cur.execute(sql_search_people, (tuple(people_names),))

    people = cur.fetchall()
    db_connections.putconn(conn)

    return people
