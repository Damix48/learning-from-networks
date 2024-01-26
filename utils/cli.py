import argparse
import populate_db
import create_dataset
import re
import datetime
import os
import dotenv

dotenv.load_dotenv()

# Initialize parser
parser = argparse.ArgumentParser()

# Adding subparsers
subparsers = parser.add_subparsers(dest="command")

# Adding db parser
db_parser = subparsers.add_parser("db")
db_parser.add_argument(
    "-r", "--reset", action="store_true", default=False, help="Reset DB"
)

# Adding dataset parser
dataset_parser = subparsers.add_parser("dataset")
dataset_parser.add_argument("-p", "--path", help="Enter dataset(s) path")

# Read arguments from command line
args = parser.parse_args()

if __name__ == "__main__":
    if args.command == "db":
        api_key = os.getenv("API_KEY")
        populate_db.populate_db(api_key, args.reset)
    elif args.command == "dataset":
        path = args.path if args.path is not None else "./dataset"

        if os.path.exists(path) == False:
            os.makedirs(path)

        create_dataset.create_movies_dataset(path)
        create_dataset.create_people_dataset(path)
        create_dataset.create_movie_connections_dataset(path)
        create_dataset.create_people_connections_dataset(path)
