import re
import pandas as pd

def matching_counter(comment: str, single_vocab: list[str], compound_vocab: list[str]) -> float:
    """Count the number of matches of words in a comment in the given vocabulary."""
    if not isinstance(comment,str) or not comment:
        return 0, 1.0
    
    try:
        words = re.findall(r'\w+', comment.lower())
    except:
        return 0, 1.0

    if len("words") == 0:
        return 0, 1.0
    
    matches = sum(1 for w in words if w in single_vocab)
    for phrase in compound_vocab:
        if phrase in comment:
            matches += 1

    return matches, len(words)

def word_frequencies(comment: str, single_vocab: list[str], compound_vocab: list[str], stopwords: set[str]) -> dict[str,dict[str,int|bool]]:
    """Map frequency of matches of words in a comment in the given vocabulary."""
    if not isinstance(comment,str) or not comment:
        return {}
    
    try:
        words = re.findall(r'\w+', comment.lower())
    except:
        return {}

    if len("words") == 0:
        return {}
    
    result = {}
    for word in words:
        if word in stopwords:
            continue

        if word not in result:
            result[word] = {"frequency": 0, "is_cohesion": word in single_vocab}
        
        result[word]["frequency"] += 1
    
    for vocab in compound_vocab:
        frequency = comment.count(vocab)
        if frequency > 0:
            result[vocab] = {"frequency": frequency, "is_cohesion": True}

    return result

def cohesion_metric(data: pd.DataFrame, cohesion_vocab: list[str]) -> dict[pd.DataFrame]:
    """Calculate cohesion score for each post."""
    data = data.copy()

    single_vocab = [word for word in cohesion_vocab if " " not in word]
    compound_vocab = [word for word in cohesion_vocab if " " in word]

    results = data['body'].apply(
        lambda x: matching_counter(x, single_vocab, compound_vocab)
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

def get_cloud_word(data: pd.DataFrame, cohesion_vocab: list[str], stopwords: set[str]):
    """Get the cloud of words of for each post."""
    data = data.copy()

    single_vocab = [word for word in cohesion_vocab if " " not in word]
    compound_vocab = [word for word in cohesion_vocab if " " in word]
    
    print(f"processing {len(data)} comments.")

    formatted_data = []
    for index, row in data.iterrows():
        if index%5000 == 4999:
            print(f"Successfully processed comment {index+1}.")
            break
        frequencies = word_frequencies(row["body"], single_vocab, compound_vocab, stopwords)

        formatted_data.extend([
            {
                "movie": row["movie"],
                "comment_id": row["comment_id"],
                "post_id": row["post_id"],
                "word": word,
                "frequency": frequencies[word]["frequency"],
                "is_cohesion": frequencies[word]["is_cohesion"],
            }
            for word in frequencies
        ])

    return formatted_data
