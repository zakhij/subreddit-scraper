from src.data_ingestor import DataIngestor
from src.database_manager import DatabaseManager
from src.data_displayer import DataDisplayer

import logging
import argparse
import praw
from datetime import datetime

from src.models import Config

_logger = logging.getLogger(__name__)


def main() -> None:
    # Configure logging
    logging.basicConfig(level=logging.INFO)

    # Load environment variables from .env file
    config = Config()

    # Use Reddit API credentials to initialize reddit instance
    reddit = praw.Reddit(
        client_id=config.REDDIT_CLIENT_ID,
        client_secret=config.REDDIT_SECRET_ID,
        user_agent=config.USER_AGENT,
    )

    # Use database connection details to initialize database manager
    databaseManager = DatabaseManager(
        config.DB_HOST, config.DB_USER, config.DB_PASSWORD, config.DB_NAME
    )

    # Initialize our data ingestor and data displayer
    dataIngestor = DataIngestor(reddit)
    dataDisplayer = DataDisplayer(databaseManager)

    # Examine and parse inputs from command line arguments
    input = _parse_input()
    _logger.info("Parsing threads from subreddit %s since %s", input[0], input[1])
    subreddit_name = input[0]
    date = input[1]

    # Ingest data into the database
    dataIngestor.ingest_data_into_database(subreddit_name, date, databaseManager)

    # Display data from the database
    dataDisplayer.display_subreddit_threads(subreddit_name, date)


def _parse_input() -> tuple[str, datetime]:
    """
    Extracts the subreddit name and date from the command line arguments.

    Raises:
        ValueError: If date is invalid (i.e., wrong formatting or is the future)

    Returns:
        A tuple of the subreddit name and date
    """
    # Set command line argument parser
    parser = argparse.ArgumentParser(description="Scrape a subreddit using Reddit API.")
    parser.add_argument(
        "--lookback_date",
        type=str,
        required=True,
        help="Date to scrape back to (e.g., 2024-01-01)",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--subreddit", type=str, help="Name of the subreddit to scrape")
    group.add_argument(
        "--subreddit_url", type=str, help="URL of the subreddit to scrape"
    )

    # Parse command line arguments
    args = parser.parse_args()

    # Determine subreddit name
    subreddit_name = None
    if args.subreddit:
        subreddit_name = args.subreddit
    elif args.subreddit_url:
        subreddit_name = args.subreddit_url.split("/")[-2]
    if subreddit_name is None:
        raise ValueError("Either --subreddit or --subreddit_url must be provided")

    # Determine lookback date
    date = args.lookback_date
    datetime_object = datetime.strptime(date, "%Y-%m-%d")
    if datetime_object > datetime.now():
        raise ValueError("Date must be in the past")

    return (subreddit_name, datetime_object)


if __name__ == "__main__":
    main()
