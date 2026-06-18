import requests
from bs4 import BeautifulSoup


def scrape_gorafi(url: str = "https://www.legorafi.fr/") -> list[str]:
    """
    Scrape Gorafi homepage for article titles.
    Returns a list of article titles.
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
    titles = []

    # Find all list items in the mega list
    list_items = soup.select("ul.mvp-mega-list li")

    for item in list_items:
        # Extract title from ul.mvp-mega-list li a p
        title_tag = item.select_one("a p")
        if title_tag:
            title = title_tag.get_text(strip=True)
            titles.append(title)

    return titles


if __name__ == "__main__":
    print("Scraping Gorafi...")
    results = scrape_gorafi()

    print(f"\nFound {len(results)} articles:")
    for i, title in enumerate(results, 1):
        print(f"\n{i}. {title}")