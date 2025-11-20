import os
import praw
import pandas as pd

from datetime import datetime
from dotenv import load_dotenv


def get_reddit_instance() -> praw.Reddit:
    """
    Create and return an authenticated Reddit instance.

    Returns:
    - praw.Reddit: Authenticated Reddit instance
    """
    load_dotenv()

    reddit = praw.Reddit(
        client_id=os.getenv('REDDIT_CLIENT_ID'),
        client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
        user_agent=os.getenv('REDDIT_USER_AGENT')
    )

    return reddit


def fetch_posts(reddit: praw.Reddit, subreddit_name: str, sort_method: str = 'hot', limit: int = 100, time_filter: str = 'all') -> pd.DataFrame:
    """
    Fetch posts from a subreddit.

    Parameters:
    - reddit: praw.Reddit instance
    - subreddit_name: Name of the subreddit
    - sort_method: 'hot', 'new', 'top', 'rising', 'controversial'
    - limit: Number of posts to fetch
    - time_filter: 'hour', 'day', 'week', 'month', 'year', 'all' (for top/controversial)

    Returns:
    - DataFrame with post data
    """
    subreddit = reddit.subreddit(subreddit_name)
    posts_data = []

    # Get posts based on sort method
    if sort_method == 'hot':
        posts = subreddit.hot(limit=limit)
    elif sort_method == 'new':
        posts = subreddit.new(limit=limit)
    elif sort_method == 'top':
        posts = subreddit.top(time_filter=time_filter, limit=limit)
    elif sort_method == 'rising':
        posts = subreddit.rising(limit=limit)
    elif sort_method == 'controversial':
        posts = subreddit.controversial(time_filter=time_filter, limit=limit)
    else:
        raise ValueError(f"Invalid sort_method: {sort_method}")

    for post in posts:
        posts_data.append({
            'post_id': post.id,
            'title': post.title,
            'author_id': getattr(post.author, 'id', None) if post.author else None,
            'author_name': getattr(post.author, 'name', '[deleted]') if post.author else '[deleted]',
            'subreddit': post.subreddit.display_name,
            'score': post.score,
            'upvote_ratio': post.upvote_ratio,
            'num_comments': post.num_comments,
            'created_utc': post.created_utc,
            'created_datetime': datetime.fromtimestamp(post.created_utc),
            'is_self': post.is_self,
            'selftext': post.selftext,
            'url': post.url,
            'permalink': f"https://reddit.com{post.permalink}",
            'flair': post.link_flair_text,
            'stickied': post.stickied,
            'locked': post.locked,
            'spoiler': post.spoiler,
            'nsfw': post.over_18
        })

    return pd.DataFrame(posts_data)


def fetch_comments_from_posts(reddit: praw.Reddit, posts_df: pd.DataFrame, max_comments_per_post: int = None) -> pd.DataFrame:
    """
    Fetch all comments from a list of posts.

    Parameters:
    - reddit: praw.Reddit instance
    - posts_df: DataFrame with post_id column
    - max_comments_per_post: Limit comments per post (None = all)

    Returns:
    - DataFrame with comment data
    """
    comments_data = []

    for idx, post_id in enumerate(posts_df['post_id'], 1):
        print(f"Processing post {idx}/{len(posts_df)}: {post_id}", end='\r')

        submission = reddit.submission(id=post_id)
        submission.comments.replace_more(
            limit=0
        )  # Remove MoreComments objects

        comment_count = 0
        for comment in submission.comments.list():
            if max_comments_per_post and comment_count >= max_comments_per_post:
                break

            # Get parent info
            parent_type = 'post' if comment.parent_id.startswith(
                't3_'
            ) else 'comment'
            parent_id = comment.parent_id.replace('t3_', '').replace('t1_', '')

            comments_data.append({
                'comment_id': comment.id,
                'post_id': submission.id,
                'author_id': comment.author.id if comment.author else None,
                'author_name': comment.author.name if comment.author else '[deleted]',
                'body': comment.body,
                'score': comment.score,
                'created_utc': comment.created_utc,
                'created_datetime': datetime.fromtimestamp(comment.created_utc),
                'parent_id': parent_id,
                'parent_type': parent_type,
                'is_submitter': comment.is_submitter,
                'stickied': comment.stickied,
                'depth': comment.depth,
                'controversiality': comment.controversiality,
                'gilded': comment.gilded
            })
            comment_count += 1

    return pd.DataFrame(comments_data)


def fetch_user_info(reddit: praw.Reddit, user_ids: list) -> pd.DataFrame:
    """
    Fetch detailed information about users.

    Parameters:
    - reddit: praw.Reddit instance
    - user_ids: List of user IDs

    Returns:
    - DataFrame with user data
    """
    users_data = []
    unique_users = list(set(user_ids))

    for idx, user_id in enumerate(unique_users, 1):
        if user_id is None:
            continue

        print(f"Fetching user {idx}/{len(unique_users)}", end='\r')

        try:
            redditor = reddit.redditor(id=user_id)
            users_data.append({
                'user_id': redditor.id,
                'username': redditor.name,
                'link_karma': redditor.link_karma,
                'comment_karma': redditor.comment_karma,
                'total_karma': redditor.total_karma,
                'created_utc': redditor.created_utc,
                'created_datetime': datetime.fromtimestamp(redditor.created_utc),
                'is_gold': redditor.is_gold,
                'is_mod': redditor.is_mod,
                'is_employee': redditor.is_employee,
                'has_verified_email': redditor.has_verified_email
            })
        except Exception as e:
            print(f"\nError fetching user {user_id}: {e}")
            continue

    return pd.DataFrame(users_data)


def build_interaction_network(comments_df: pd.DataFrame, posts_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build user interaction network from comments.
    Creates edges between users based on reply patterns.

    Parameters:
    - comments_df: DataFrame with comment data
    - posts_df: DataFrame with post data

    Returns:
    - DataFrame with edge list (from_user, to_user, interaction_type, weight)
    """
    interactions = []

    # Create lookup for post authors
    post_authors = posts_df.set_index('post_id')['author_id'].to_dict()

    # Create lookup for comment authors
    comment_authors = comments_df.set_index(
        'comment_id'
    )['author_id'].to_dict()

    for _, comment in comments_df.iterrows():
        if comment['author_id'] is None:
            continue

        from_user = comment['author_id']
        to_user = None
        interaction_type = None

        if comment['parent_type'] == 'post':
            # Comment on a post
            to_user = post_authors.get(comment['parent_id'])
            interaction_type = 'comment_on_post'
        else:
            # Reply to a comment
            to_user = comment_authors.get(comment['parent_id'])
            interaction_type = 'reply_to_comment'

        if to_user and from_user != to_user:  # Exclude self-interactions
            interactions.append({
                'from_user_id': from_user,
                'to_user_id': to_user,
                'interaction_type': interaction_type,
                'timestamp': comment['created_utc'],
                'comment_id': comment['comment_id'],
                'post_id': comment['post_id']
            })

    interactions_df = pd.DataFrame(interactions)

    # Aggregate interactions (weight edges)
    if len(interactions_df) > 0:
        edge_list = interactions_df.groupby(
            ['from_user_id', 'to_user_id', 'interaction_type']
        ).agg({
            'comment_id': 'count',
            'timestamp': 'min'
        }).reset_index()
        edge_list.columns = [
            'from_user_id', 'to_user_id',
            'interaction_type', 'weight', 'first_interaction'
        ]
    else:
        edge_list = pd.DataFrame(columns=[
            'from_user_id', 'to_user_id', 'interaction_type',
            'weight', 'first_interaction'
        ])

    return edge_list
