import numpy as np
import pandas as pd

def overlapping_users(data: pd.DataFrame, is_global: bool = False) -> dict[str,pd.DataFrame]:
    """Calculate users from a post that interact in other posts within a given film."""
    data = data.copy()
    data = data[data["author_name"] != "[deleted]"]

    if is_global:
        film_subgroups = data.groupby(lambda _: 0)
    else:
        film_subgroups = data.groupby("movie")

    results = {}
    for identifier, film_group in film_subgroups:
        post_subgroups = film_group.groupby("post_id")
        post_authors = post_subgroups.agg({"author_name": "unique"})
        post_authors["author_name"] = post_authors["author_name"].apply(set)

        author_subgroups = film_group.groupby("author_name")
        author_posts_count = author_subgroups.agg({"post_id": "nunique"})
        multiauthors = author_posts_count[author_posts_count["post_id"]>1].index.tolist()

        multiauthor_counters = {}
        for post_id, authors in post_authors["author_name"].items():
            if post_id not in multiauthor_counters:
                multiauthor_counters[post_id] = 0

            for author in authors:
                if author in multiauthors:
                    multiauthor_counters[post_id] += 1
        
        post_authors["multiauthors"] = post_authors.index.map(multiauthor_counters)
        results[identifier] = post_authors

    return results
