from bs4 import BeautifulSoup
from pathlib import Path

def extract_post_id(url: str) -> str:
    """Extract the id of the url."""
    id = url.split("comments/", maxsplit=1)[1]
    return id.split("/",maxsplit=1)[0]


def get_ids_list(file_path: str) -> list[str]:
    """Extract ids present in Reddit posts' URLs in an HTML."""
    file_path = Path(file_path)
    movies_content = file_path.read_text("utf-8")
    soup = BeautifulSoup(movies_content, 'html.parser')

    ids = []
    current_movie = ""
    for ol in soup.find_all("ol"):
        li = ol.find_next("li")
        if li:
            span = li.find_next("span")
            if span and span.get("class", [""])[0] == "c5":
                current_movie = span.text
                continue

        for link in ol.find_all("a"):
            link_text = link.get("href")
            if "reddit" in link_text:
                ids.append((extract_post_id(link_text), current_movie))

    return ids
