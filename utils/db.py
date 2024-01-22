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
    release_date DATE
);
"""

sql_create_actors = """
-- Create the "actors" table
CREATE TABLE actors (
    id INTEGER PRIMARY KEY,
    name TEXT,
    popularity REAL
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

sql_create_movie_actor_collaboration = """
-- Create the "movie_actor_collaboration" table to represent the many-to-many relationship between movies and actors
CREATE TABLE movie_actor_collaboration (
    movie_id INTEGER,
    actor_1_id INTEGER,
    actor_2_id INTEGER,
    FOREIGN KEY (movie_id) REFERENCES movies(id),
    FOREIGN KEY (actor_1_id) REFERENCES actors(id),
    FOREIGN KEY (actor_2_id) REFERENCES actors(id),
    PRIMARY KEY (movie_id, actor_1_id, actor_2_id)
);
"""


def reset_db():
    delete_db()

    create_db()


def create_db():
    conn = psycopg2.connect(
        dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port
    )
    cur = conn.cursor()

    cur.execute(sql_create_movies)
    cur.execute(sql_create_actors)
    cur.execute(sql_create_movie_actor)
    cur.execute(sql_create_movie_actor_collaboration)

    conn.commit()

    cur.close()
    conn.close()


def delete_db():
    conn = psycopg2.connect(
        dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port
    )
    cur = conn.cursor()

    cur.execute("DROP TABLE movie_actor_collaboration")
    cur.execute("DROP TABLE movie_actor")
    cur.execute("DROP TABLE movies")
    cur.execute("DROP TABLE actors")

    conn.commit()

    cur.close()
    conn.close()


def insert_movies(movies: List[Tuple[int, str]]):
    print(f"Inserting {len(movies)} movies")

    conn = db_connections.getconn()
    cur = conn.cursor()

    cur.executemany("INSERT INTO movies VALUES (%s, %s, %s)", movies)

    conn.commit()

    cur.close()
    db_connections.putconn(conn)


def insert_actors(actors: List[Tuple[int, str, float]]):
    print(f"Inserting {len(actors)} actors")

    conn = db_connections.getconn()
    cur = conn.cursor()

    cur.executemany(
        "INSERT INTO actors VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING", actors
    )

    conn.commit()

    cur.close()
    db_connections.putconn(conn)


def insert_movie_actor(movie_actor: List[Tuple[int, int]]):
    print(f"Inserting {len(movie_actor)} movie_actor")

    conn = db_connections.getconn()
    cur = conn.cursor()

    cur.executemany("INSERT INTO movie_actor VALUES (%s, %s)", movie_actor)

    conn.commit()

    cur.close()
    db_connections.putconn(conn)


def insert_movie_actor_collaboration(actor_collaboration: List[Tuple[int, int, int]]):
    print(f"Inserting {len(actor_collaboration)} movie_actor_collaboration")

    conn = db_connections.getconn()
    cur = conn.cursor()

    cur.executemany(
        "INSERT INTO movie_actor_collaboration VALUES (%s, %s, %s)", actor_collaboration
    )

    conn.commit()

    cur.close()
    db_connections.putconn(conn)


def get_movies(
    start_date: Union[datetime.datetime, str] = datetime.date.min,
    end_date: Union[datetime.datetime, str] = datetime.date.max,
):
    conn = db_connections.getconn()
    cur = conn.cursor()

    query = """
    SELECT *
    FROM movies
    WHERE release_date BETWEEN %s AND %s;
    """

    cur.execute(query, (start_date, end_date))

    movies = cur.fetchall()

    cur.close()
    db_connections.putconn(conn)

    return movies


def get_actors():
    conn = db_connections.getconn()
    cur = conn.cursor()

    cur.execute("SELECT * FROM actors")

    actors = cur.fetchall()

    cur.close()
    db_connections.putconn(conn)

    return actors


def get_movie_actor():
    conn = db_connections.getconn()
    cur = conn.cursor()

    cur.execute("SELECT * FROM movie_actor")

    movie_actor = cur.fetchall()

    cur.close()
    db_connections.putconn(conn)

    return movie_actor


def get_movie_actor_collaboration(
    start_date: Union[datetime.datetime, str] = datetime.date.min,
    end_date: Union[datetime.datetime, str] = datetime.date.max,
    min_popularity: float = 0.6,
    epsilon: float = 0.0001,
):
    conn = db_connections.getconn()
    cur = conn.cursor()

    query = """
    SELECT
        mac.*
    FROM
        movie_actor_collaboration AS mac
    JOIN
        movies AS m ON mac.movie_id = m.id
    JOIN
        actors AS a1 ON mac.actor_1_id = a1.id
    JOIN
        actors AS a2 ON mac.actor_2_id = a2.id
    WHERE
        m.release_date BETWEEN %s AND %s
        AND a1.popularity > %s
        AND a2.popularity > %s;
    """

    start_date = (
        isinstance(start_date, datetime.datetime)
        and start_date.isoformat()
        or start_date
    )
    end_date = (
        isinstance(end_date, datetime.datetime) and end_date.isoformat() or end_date
    )

    cur.execute(
        query,
        (start_date, end_date, min_popularity + epsilon, min_popularity + epsilon),
    )

    movie_actor_collaboration = cur.fetchall()

    cur.close()
    db_connections.putconn(conn)

    return movie_actor_collaboration


def get_actors_movies(
    start_date: Union[datetime.datetime, str] = datetime.date.min,
    end_date: Union[datetime.datetime, str] = datetime.date.max,
    min_popularity: float = 0.6,
    epsilon: float = 0.0001,
):
    conn = db_connections.getconn()
    cur = conn.cursor()

    query = """
    SELECT
        a.id AS actor_id,
        a.name AS actor_name,
        a.popularity AS actor_popularity,
        COUNT(ma.movie_id) AS movie_count
    FROM
        actors AS a
    JOIN
        movie_actor AS ma ON a.id = ma.actor_id
    JOIN
        movies AS m ON ma.movie_id = m.id
    WHERE
        m.release_date BETWEEN %s AND %s
        AND a.popularity > %s
    GROUP BY
        a.id, a.name, a.popularity;
    """

    start_date = (
        isinstance(start_date, datetime.datetime)
        and start_date.isoformat()
        or start_date
    )
    end_date = (
        isinstance(end_date, datetime.datetime) and end_date.isoformat() or end_date
    )

    cur.execute(
        query,
        (start_date, end_date, min_popularity + epsilon),
    )

    actors_movies = cur.fetchall()

    cur.close()
    db_connections.putconn(conn)

    return actors_movies
