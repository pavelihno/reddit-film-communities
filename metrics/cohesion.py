import re
import pandas as pd

def matching_counter(comment: str, single_vocab: list[str], comppound_vocab: list[str]) -> float:
    """Count the number of matches of words in a comment in the given vocabulary."""
    if not isinstance(comment,str) or not comment:
        return 0, 1.0
    
    try:
        words = re.findall(r'\w+', comment.lower())
    except:
        print("a")

    if len("words") == 0:
        return 0, 1.0
    
    matches = sum(1 for w in words if w in single_vocab)
    for phrase in comppound_vocab:
        if phrase in comment:
            matches += 1

    return matches, len(words)

def cohesion_metric(data: pd.DataFrame, cohesion_vocab: list[str]) -> dict[pd.DataFrame]:
    """Calculate cohesion score for each post."""
    data = data.copy()

    single_vocab = [word for word in cohesion_vocab if " " not in word]
    compound_vocab = [word for word in cohesion_vocab if " " in word]

    results = data['body'].apply(
        lambda x: matching_counter(x, single_vocab, cohesion_vocab)
    )

    data["cohesion_matches"] = [x for x,_ in results]
    data["total_words"] = [y for _,y in results]

    post_subgroups = data.groupby("post_id")
    summaries = post_subgroups.agg({
        "cohesion_matches": "sum",
        "total_words": "sum",
    })
    summaries["cohesion_score"] = summaries["cohesion_matches"]/summaries["total_words"]

    return summaries