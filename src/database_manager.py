import pymysql
import datetime
import pymysql.cursors

from src.models import Thread, Subreddit, Comment


class DatabaseManager:
    """
    Manages interactions with the MySQL database using PyMySQL.
    """

    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database_name = database
        self.connection = self.connect()

    def connect(self) -> pymysql.Connection:
        """
        Provides a connection to the MySQL database.

        Returns:
            A connection to the MySQL database using PyMySQL
        """
        return pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database_name,
            cursorclass=pymysql.cursors.DictCursor,
        )

    def __enter__(self) -> "DatabaseManager":
        """
        Upon entering, if a connection is not open, open one.

        Returns:
            Self
        """
        if self.connection is None or self.connection.open is False:
            self.connection = self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """
        Upon exiting, if a connection is open, close it.
        """
        if self.connection and self.connection.open:
            self.connection.close()

    def insert_subreddit_data(self, subreddit: Subreddit) -> None:
        """
        Inserts or updates the subreddit data into the database.

        Args:
            subreddit: The subreddit object to insert/update
        """

        insert_subreddit_data_query = """
        INSERT INTO subreddits (id, name)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE name=%s
        """
        with self.connection.cursor() as cursor:
            cursor.execute(
                insert_subreddit_data_query,
                (subreddit.id, subreddit.name, subreddit.name),
            )

    def insert_thread_data(self, thread: Thread) -> None:
        """
        Inserts or updates the thread data into the database.

        Args:
            thread: The thread object to insert/update
        """

        insert_thread_query = """
        INSERT INTO threads (id, subreddit_id, title, text, external_url, url, username, upvotes, date_posted)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE upvotes=%s, title=%s, url=%s, text=%s
        """
        with self.connection.cursor() as cursor:
            cursor.execute(
                insert_thread_query,
                (
                    thread.id,
                    thread.subreddit_id,
                    thread.title,
                    thread.text,
                    thread.external_url,
                    thread.url,
                    thread.username,
                    thread.upvotes,
                    thread.date_posted,
                    thread.upvotes,
                    thread.title,
                    thread.url,
                    thread.text,
                ),
            )

    def insert_comment_data(self, comments: list[Comment]) -> None:
        """
        Inserts or updates the comment data into the database.

        Args:
            comments: The list of comment objects to insert/update
        """
        insert_comment_query = """
        INSERT INTO comments (id, parent_comment_id, thread_id, username, upvotes, date_posted, text)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE upvotes=%s, text=%s
        """

        with self.connection.cursor() as cursor:
            for comment in comments:
                cursor.execute(
                    insert_comment_query,
                    (
                        comment.id,
                        comment.parent_comment_id,
                        comment.thread_id,
                        comment.username,
                        comment.upvotes,
                        comment.date_posted,
                        comment.text,
                        comment.upvotes,
                        comment.text,
                    ),
                )

    def get_subreddit_id(self, subreddit_name: str) -> str | None:
        """
        Retrieves the subreddit ID from the database based on the subreddit name.

        Args:
            subreddit_name: The name of the subreddit

        Returns:
            String of the subreddit ID if found, else None
        """
        with self.connection.cursor() as cursor:
            cursor.execute(
                "SELECT id FROM subreddits WHERE name = %s", (subreddit_name,)
            )
            result = cursor.fetchone()
            return result["id"] if result else None

    def get_threads(self, subreddit_id: str, start_date: datetime.date) -> list[Thread]:
        """
        Retrieves all rows of thread data from the database based on the subreddit ID and start date.

        Args:
            subreddit_id: The ID of the subreddit
            start_date: The start date to fetch threads from

        Returns:
            A list of threads fetched from the database
        """
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, subreddit_id, title, text, url, external_url, username, upvotes, date_posted 
                FROM threads 
                WHERE subreddit_id = %s AND date_posted >= %s
                ORDER BY date_posted DESC
            """,
                (subreddit_id, start_date),
            )
            threads = [Thread(**row) for row in cursor.fetchall()]
            return threads

    def get_comments(self, thread_id: str) -> list[Comment]:
        """
        Retrieves all rows of comment data from the database based on the thread.

        Args:
            thread_id: The ID of the thread the comments belong to.

        Returns:
            A list of comments fetched from the database
        """
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, thread_id, parent_comment_id, username, upvotes, date_posted, text 
                FROM comments 
                WHERE thread_id = %s
                ORDER BY date_posted ASC
            """,
                (thread_id,),
            )
            comments = [Comment(**row) for row in cursor.fetchall()]
            return comments
