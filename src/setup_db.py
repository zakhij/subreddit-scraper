import pymysql
import pymysql.cursors
from src.models import Config
import logging

logger = logging.getLogger(__name__)


def main():
    logging.basicConfig(level=logging.INFO)
    create_database_and_tables()


def create_database_and_tables() -> None:
    """
    Creates the necessary database and tables for the Reddit scraper if they don't exist.
    """

    # Load environment variables from .env file
    config = Config()

    # For type safety
    assert config.DB_PASSWORD is not None

    connection = pymysql.connections.Connection(
        host=config.DB_HOST,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )

    with connection.cursor() as cursor:
        # Create the database
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {config.DB_NAME};")
        cursor.execute(f"USE {config.DB_NAME};")

        # Create subreddits table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS subreddits (
                id VARCHAR(255) PRIMARY KEY,
                name VARCHAR(255) NOT NULL
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """
        )

        # Create threads table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS threads (
                id VARCHAR(255) PRIMARY KEY,
                subreddit_id VARCHAR(255),
                title TEXT NOT NULL,
                text TEXT,
                external_url TEXT,
                url TEXT NOT NULL,
                username VARCHAR(255),
                upvotes INT,
                date_posted DATETIME,
                FOREIGN KEY (subreddit_id) REFERENCES subreddits(id) ON DELETE CASCADE
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """
        )

        # Create comments table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS comments (
                id VARCHAR(255) PRIMARY KEY,
                thread_id VARCHAR(255),
                parent_comment_id VARCHAR(255),
                username VARCHAR(255),
                upvotes INT,
                date_posted DATETIME,
                text TEXT,
                FOREIGN KEY (thread_id) REFERENCES threads(id) ON DELETE CASCADE,
                FOREIGN KEY (parent_comment_id) REFERENCES comments(id) ON DELETE CASCADE
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """
        )

        connection.commit()
        logger.info("Database and tables created successfully.")


# Run the script
if __name__ == "__main__":
    main()
