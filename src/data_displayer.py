from src.database_manager import DatabaseManager

from datetime import datetime
from rich.console import Console
from rich.tree import Tree
from rich.panel import Panel

from src.models import Comment, Thread


class DataDisplayer:
    """Class for displaying data from the database"""

    def __init__(self, database_manager: DatabaseManager) -> None:
        self.database = database_manager
        self.console = Console()

    def display_subreddit_threads(
        self, subreddit_name: str, start_date: datetime
    ) -> None:
        """
        Main function to retrieve and display threads/comments from a specific subreddit,
        starting from the given date.

        Args:
            subreddit_name: Subreddit name to fetch threads from
            start_date: Start date to fetch threads from
        """
        with self.database as database:
            subreddit_id = database.get_subreddit_id(subreddit_name)

            # If subreddit_id is None, then the subreddit was not found in the database. Log a message and return
            if not subreddit_id:
                self.console.log(
                    f"[red]Subreddit '{subreddit_name}' not found in the database.[/red]"
                )
                return

            # Fetch threads from the database
            threads = database.get_threads(subreddit_id, start_date)

            # If no threads were found, then log a message and return
            if not threads:
                self.console.log(
                    f"[yellow]No threads found for subreddit '{subreddit_name}' since {start_date}.[/yellow]"
                )
                return

            # Display the threads and their comments
            for thread in threads:
                self.display_thread(thread)
                self.console.print("\n\n")

    def display_thread(self, thread: Thread) -> None:
        """
        Displays a thread and its associated comments into the console.
        Thread information is displayed in a panel with the comments below it.

        Args:
            thread: A dictionary containing the thread data
        """
        panel_title = f"{thread.title} (by {thread.username})"
        panel_content = (
            f"[bold]Upvotes:[/bold] {thread.upvotes}\n"
            f"[bold]Posted on:[/bold] {thread.date_posted}\n"
        )

        # Add the text if it's present and not empty
        if thread.text:
            panel_content += f"\n{thread.text}\n"

        # Add the external URL if it's present
        if thread.external_url:
            panel_content += f"\n[bold]External URL:[/bold] {thread.external_url}\n"

        # Always include the thread URL
        panel_content += f"\n[bold]Thread URL:[/bold] {thread.url}"

        self.console.print(Panel(panel_content, title=panel_title, expand=True))

        # Retrieve and display comments
        comments = self.database.get_comments(thread.id)
        if comments:
            self.display_comments(comments)

    def display_comments(self, comments: list[Comment]) -> None:
        """
        Displays comments in a hierarchical tree structure in the console.
        Hierarchy is determined by the parent_comment_id of each comment.
        Assumes that comments are processed in chronological order.

        Args:
            comments: A list of dictionaries containing the comment data
        """

        comment_tree = Tree("[bold]Comments:[/bold]")
        comment_dict = {None: comment_tree}

        for comment in comments:
            comment_text = (
                f"{comment.username} (Upvotes: {comment.upvotes})\n"
                f"[italic]{comment.date_posted}[/italic]\n"
                f"{comment.text}"
            )
            if comment.parent_comment_id:
                parent_node = comment_dict.get(comment.parent_comment_id)
                comment_dict[comment.id] = parent_node.add(comment_text)
            else:
                comment_dict[comment.id] = comment_tree.add(comment_text)

        self.console.print(comment_tree)
