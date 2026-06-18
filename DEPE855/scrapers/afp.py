import requests
from bs4 import BeautifulSoup
from datetime import datetime


def scrape_afp_factcheck(url: str = "https://factcheck.afp.com/") -> list[dict]:
    """
    Scrape AFP Fact Check for articles.
    Returns a list of dictionaries with 'title' and 'date' keys.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    articles = []

    # Find all editor-choice containers
    containers = soup.select("div.editor-choice")

    for container in containers:
        # Extract title from h3
        title_tag = container.select_one("h3")
        title = title_tag.get_text(strip=True) if title_tag else "No title"

        # Extract date from div.date-short-format with data-utc-time attribute
        date_tag = container.select_one("div.date-short-format")
        if date_tag and date_tag.has_attr("data-utc-time"):
            # Parse ISO format date from data-utc-time attribute
            date_str = date_tag["data-utc-time"]
            try:
                # Handle both full ISO and date-only formats
                if "T" in date_str:
                    date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                else:
                    date = datetime.fromisoformat(date_str)
                date = date.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                date = date_str  # Fallback to raw string
        else:
            date = "No date"

        articles.append({"title": title, "date": date})

    return articles


if __name__ == "__main__":
    print("Scraping AFP Fact Check...")
    results = scrape_afp_factcheck()

    print(f"\nFound {len(results)} articles:")
    for i, article in enumerate(results, 1):
        print(f"\n{i}. {article['title']}")
        print(f"   Date: {article['date']}")