# Learning from Networks

## Setup

Requirements:

- PostgreSQL database
- TMDB api key: [https://developer.themoviedb.org/](https://developer.themoviedb.org/)

First steps:

- Create a `.env` file modifing `.env.sample`
- Update the `.env` file with your correct values

## Usage

To populate the database, the `reset` flag is needed for the first time and when you want to recreate it from scratch:
```bash
python ./utils/cli.py db [--reset]
```

To create the datasets csv files, the `p` arguments for specify a different folder for the files:
```bash
python ./utils/cli.py dataset [-p PATH]
```