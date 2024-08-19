from dataclasses import dataclass, field
from typing import List, Optional
from dotenv import load_dotenv
import os


class Config:
    def __init__(self):
        load_dotenv()
        self.REDDIT_CLIENT_ID: str | None = os.getenv("REDDIT_CLIENT_ID")
        self.REDDIT_SECRET_ID: str | None = os.getenv("REDDIT_SECRET_ID")
        self.USER_AGENT: str | None = os.getenv("USER_AGENT")
        self.DB_HOST: str | None = os.getenv("DB_HOST")
        self.DB_USER: str | None = os.getenv("DB_USER")
        self.DB_PASSWORD: str | None = os.getenv("DB_PASSWORD")
        self.DB_NAME: str | None = os.getenv("DB_NAME")


@dataclass
class Comment:
    """
    Represents a Reddit comment.
    """

    id: str
    thread_id: str
    parent_comment_id: str | None
    username: str | None
    upvotes: int
    date_posted: str
    text: str


@dataclass
class Thread:
    """
    Represents a Reddit thread.
    """

    id: str
    subreddit_id: str
    title: str
    text: Optional[str]
    external_url: Optional[str]
    url: str
    username: Optional[str]
    upvotes: int
    date_posted: str
    comments: List[Comment] = field(default_factory=list)


@dataclass
class Subreddit:
    """
    Represents a Reddit subreddit.
    """

    id: str
    name: str
