import requests
import re


class WikipediaApi:
    def __init__(self) -> None:
        self.base_url = "https://en.wikipedia.org/w/api.php"

    def get_category_members(
        self, category: str, limit: int = 500, cmcontinue: str = None
    ):
        params = {
            "action": "query",
            "format": "json",
            "list": "categorymembers",
            "formatversion": 2,
            "cmtitle": category,
            "cmlimit": limit,
            "cmcontinue": cmcontinue,
        }

        r = requests.get(self.base_url, params=params).json()

        return (
            r["query"]["categorymembers"],
            r["continue"]["cmcontinue"] if "continue" in r else None,
        )

    def get_all_category_members(self, category: str, limit: int = 500):
        cmcontinue = None
        all_pages = []

        while True:
            pages, cmcontinue = self.get_category_members(category, limit, cmcontinue)
            all_pages.extend(pages)

            if cmcontinue is None:
                break

        return all_pages

    def get_all_links(self, page: str):
        params = {
            "action": "query",
            "format": "json",
            "formatversion": 2,
            "prop": "revisions",
            "rvprop": "content",
            "rvslots": "main",
            "titles": page,
        }

        r = requests.get(self.base_url, params=params).json()

        if "missing" in r["query"]["pages"][0]:
            return []

        text = r["query"]["pages"][0]["revisions"][0]["slots"]["main"]["content"]
        text = r["query"]["pages"][0]["revisions"][0]["slots"]["main"]["content"]
        re_links = re.compile(r"\[\[(.*?)\]\]")
        links = re_links.findall(text)

        return links


class TMDBApi:
    def __init__(self, api_key: str) -> None:
        self.api_key = api_key
        self.base_url = "https://api.themoviedb.org/3"
        self.headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }

    def search_movie(self, title: str, year: str = None):
        params = {
            "language": "en-US",
            "adult": "false",
            "query": title,
            "year": year,
        }

        print(params)

        r = requests.get(
            f"{self.base_url}/search/movie", headers=self.headers, params=params
        ).json()

        return r["results"][0] if len(r["results"]) > 0 else None

    def get_movie_details(self, movie_id: int):
        params = {
            "language": "en-US",
            "append_to_response": "credits,keywords",
        }

        return requests.get(
            f"{self.base_url}/movie/{movie_id}", headers=self.headers, params=params
        ).json()

    def get_person_details(self, person_id: int):
        params = {
            "language": "en-US",
        }

        return requests.get(
            f"{self.base_url}/person/{person_id}", headers=self.headers, params=params
        ).json()
