from nyt_scraper import Scraper
from typing import Optional


def search_nyt_articles(
    keyword: str,
    sort: str = "newest",
    type_: str = "",
    section: str = "",
    max_results: Optional[int] = None,
) -> list[dict]:
    """
    Search NYT articles using the nyt-scraper API.
    Returns a list of dictionaries with 'title' and 'date' keys.

    Args:
        keyword: Search term
        sort: Sorting order ("newest", "oldest", "best")
        type_: Content type filter ('article', 'recipe', 'video', etc.)
        section: Section filter ('food', 'arts', 'travel', 'business', etc.)
        max_results: Maximum number of results to return (None for all)
    """
    scraper = Scraper()

    try:
        # Perform the search
        results = scraper.search(
            keyword=keyword,
            sort=sort,
            type_=type_,
            section=section,
        )
    except Exception as e:
        print(f"Error searching NYT: {e}")
        return []

    articles = []

    for article in results:
        # Extract title
        title = getattr(article, "title", None) or getattr(article, "headline", "No title")

        # Extract date - try different possible attributes
        date = (
            getattr(article, "date", None)
            or getattr(article, "published_date", None)
            or getattr(article, "timestamp", None)
            or "No date"
        )

        # If date is an object, try to convert it to string
        if hasattr(date, "strftime"):
            date = date.strftime("%Y-%m-%d %H:%M:%S")
        elif not isinstance(date, str):
            date = str(date)

        articles.append({"title": title, "date": date})

        # Stop if max_results is reached
        if max_results and len(articles) >= max_results:
            break

    return articles


if __name__ == "__main__":
    print("Searching NYT articles...")

    # Example: Search for "election" articles, sorted by newest
    keyword = input("Enter search keyword: ").strip() or "election"
    
    results = search_nyt_articles(
        keyword=keyword,
        sort="newest",
        type_="article",
        max_results=10,
    )

    print(f"\nFound {len(results)} articles:")
    for i, article in enumerate(results, 1):
        print(f"\n{i}. {article['title']}")
        print(f"   Date: {article['date']}")