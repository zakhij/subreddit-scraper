from src.database_manager import DatabaseManager


from datetime import datetime
import praw
import logging

from src.models import Thread, Subreddit, Comment

import praw.models
import praw.models.reddit.submission
import praw.models.reddit.comment
import praw.models.reddit.more


logger = logging.getLogger(__name__)


class DataIngestor:
    """
    Class for fetching and ingesting data from the Reddit API into the database.
    """

    def __init__(self, reddit: praw.Reddit) -> None:
        self.reddit = reddit

    def ingest_data_into_database(
        self,
        subreddit_name: str,
        start_date: datetime,
        database_manager: DatabaseManager,
    ) -> None:
        """
        Calls the get_threads_since_date method with the given subreddit_name and start_date
        and commits the transaction once all threads have been fetched and inserted.

        Args:
            subreddit_name: Name of the subreddit to fetch threads from
            start_date: Start date to fetch threads from
            database_manager: DatabaseManager instance to use for data insertion
        """
        with database_manager as database:
            self.get_threads_since_date(subreddit_name, start_date, database)
            database.connection.commit()

        logger.info("Data ingested successfully.")

    def get_threads_since_date(
        self, subreddit_name: str, start_date: datetime, database: DatabaseManager
    ) -> None:
        """
        Fetches threads from the given subreddit since the start_date in batches. Iterates through each batch,
        parses through each thread and insert them into the database, one by one.
        Args:
            subreddit_name: Name of the subreddit to fetch threads from
            start_date: Start date to fetch threads from
            database: DatabaseManager instance to use for data insertion
        """

        def helper(thread: praw.models.reddit.submission.Submission) -> None:
            """
            Calls helper methods to parse through the given thread and insert its
            data into the database.

            Args:
                thread: An individual thread object fetched from the reddit API
            """
            parsed_thread = self.parse_thread(thread)
            database.insert_thread_data(parsed_thread)
            database.insert_comment_data(parsed_thread.comments)

        # Get subreddit object from the reddit API. Parse through its data and insert it into the database
        subreddit = self.reddit.subreddit(subreddit_name)
        subreddit_data = self.parse_subreddit(subreddit_name)
        database.insert_subreddit_data(subreddit_data)

        # Fetch threads in batches (100 threads at a time) and process/ingest each.
        # We break out of the loop if we come across a thread that is older than the start_date or
        # if there are no more threads to fetch.
        last_thread = None
        while True:
            batch = subreddit.new(limit=100, params={"after": last_thread})
            if not batch:
                return
            for thread in batch:
                logger.info(f"Processing thread: {thread.title}")
                if datetime.fromtimestamp(thread.created_utc) < start_date:
                    return
                helper(thread)
                last_thread = thread.fullname  # Necessary to fetch the next batch

    def parse_subreddit(self, subreddit_name: str) -> Subreddit:
        """
        Fetches subreddit data from the given subreddit and encapsulates the relevant
        data in a Subreddit dataclass object.

        Args:
            subreddit_name: Name of the subreddit to fetch threads from

        Returns:
            A Subreddit object
        """
        subreddit = self.reddit.subreddit(subreddit_name)

        return Subreddit(
            id=subreddit.id,
            name=subreddit.display_name,
        )

    def parse_thread(self, thread: praw.models.reddit.submission.Submission) -> Thread:
        """
        Parses through the given thread object and encapsulates the relevant
        data in a Thread dataclass object.

        Args:
            thread: An individual thread object fetched from the reddit API

        Returns:
            A Thread object
        """
        parsed_thread = Thread(
            id=thread.id,
            subreddit_id=thread.subreddit.id,
            title=thread.title,
            text=thread.selftext,
            external_url=thread.url if not thread.is_self else None,
            url=f"https://reddit.com{thread.permalink}",
            username=thread.author.name if thread.author else None,
            upvotes=thread.score,
            date_posted=datetime.fromtimestamp(thread.created_utc).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            comments=self.parse_comments(thread),
        )
        return parsed_thread

    def parse_comments(
        self, thread: praw.models.reddit.submission.Submission
    ) -> list[Comment]:
        """
        Fetches all comments of the given thread and parses through each,
        returning a list of Comment objects.

        Args:
            thread: An individual thread object fetched from the reddit API

        Returns:
            A list of Comment objects
        """
        thread.comments.replace_more(limit=None)

        comments = [
            self.parse_comment(comment, thread) for comment in thread.comments.list()
        ]

        return comments

    def parse_comment(
        self,
        comment: praw.models.reddit.comment.Comment,
        thread: praw.models.reddit.submission.Submission,
    ) -> Comment:
        """
        Parses through the given comment object (and its parent thread) and encapsulates the relevant
        data in a Comment dataclass object.

        Args:
            comment: A comment object fetched from the reddit API
            thread: The thread object that the comment belongs to

        Returns:
            A Comment object
        """
        return Comment(
            id=comment.id,
            thread_id=thread.id,
            parent_comment_id=self.get_parent_comment_id(comment),
            username=comment.author.name if comment.author else None,
            upvotes=comment.score,
            date_posted=datetime.fromtimestamp(comment.created_utc).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            text=comment.body,
        )

    def get_parent_comment_id(
        self, comment: praw.models.reddit.comment.Comment
    ) -> str | None:
        """
        Checks if the given comment has a parent comment id and parses it out if it does.

        Args:
            comment: A comment object fetched from the reddit API

        Returns:
            A string of the comment id of the parent if it exists, else None
        """
        if comment.parent_id is None:
            return None
        if comment.parent_id.startswith("t1_"):
            return comment.parent_id[3:]

        return None
