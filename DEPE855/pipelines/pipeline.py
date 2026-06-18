import json
import os
from datetime import datetime
from pathlib import Path
import hashlib
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa


# ==================== CONFIG ====================
RAW_DIR = "output/raw"
PROCESSED_DIR = "output/processed"
FAKE_NEWS_API_KEY = os.getenv("FAKE_NEWS_API_KEY", "")
FAKE_NEWS_API_URL = "https://api.fakenewsdetector.com/v1/score"

# ==================== HELPERS ====================

def load_raw_data(raw_dir: str) -> list[dict]:
    """Load all JSON files from output/raw/*/ directories."""
    raw_data = []
    raw_path = Path(raw_dir)

    if not raw_path.exists():
        print(f"Warning: {raw_dir} does not exist. Creating...")
        raw_path.mkdir(parents=True, exist_ok=True)
        return raw_data

    # Walk through all subdirectories
    for json_file in raw_path.glob("**/*.json"):
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    raw_data.extend(data)
                else:
                    raw_data.append(data)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"Warning: Could not load {json_file}: {e}")

    return raw_data


def standardize_date(date_str: str) -> str:
    """Convert various date formats to YYYY-MM-DD."""
    if not date_str or date_str.lower() in ("no date", "none", ""):
        return "1970-01-01"  # Default for missing dates

    # Already ISO format
    if date_str.count("-") == 2 and len(date_str) == 10:
        return date_str

    # DD/MM/YYYY or MM/DD/YYYY
    if "/" in date_str:
        parts = date_str.split("/")
        if len(parts) == 3:
            # Assume DD/MM/YYYY
            day, month, year = parts
            try:
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            except (ValueError, AttributeError):
                pass

    # Try parsing with datetime
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        pass

    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        pass

    # Fallback: return original
    print(f"Warning: Could not parse date '{date_str}', using default")
    return "1970-01-01"


def generate_run_id() -> str:
    """Generate a unique run ID based on timestamp."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def clean_text(text: str) -> str:
    """Clean and normalize text."""
    if not text:
        return ""
    return text.strip()


def get_verification_score(item: dict, afp_data: list[dict]) -> float:
    """
    Calculate verification score for an item.
    - Cross-check with AFP fact checks
    - Use Fake News API
    - Boost score if multiple sources report same event
    """
    base_score = 0.5  # Neutral starting point

    # Check against AFP fact checks
    afp_verified = False
    for afp_item in afp_data:
        if (afp_item.get("title") == item.get("title") and
                afp_item.get("date") == item.get("date")):
            # If AFP marks it as true, trust it
            afp_rating = afp_item.get("rating", "").lower()
            if "true" in afp_rating or "real" in afp_rating:
                base_score = 0.95
                afp_verified = True
            elif "false" in afp_rating or "fake" in afp_rating:
                base_score = 0.05
                afp_verified = True
            break

    # Call Fake News Detector API
    fake_news_score = 0.5
    if FAKE_NEWS_API_KEY and item.get("title"):
        try:
            import requests
            response = requests.post(
                FAKE_NEWS_API_URL,
                json={"text": item["title"]},
                headers={"Authorization": f"Bearer {FAKE_NEWS_API_KEY}"},
                timeout=10
            )
            if response.status_code == 200:
                result = response.json()
                fake_news_score = result.get("real_probability", 0.5)
        except Exception as e:
            print(f"Warning: Fake News API error: {e}")

    # Combine scores
    verification_score = (base_score + fake_news_score) / 2

    # Boost if AFP verified
    if afp_verified:
        verification_score = base_score  # Use AFP's determination

    return round(verification_score, 2)


def count_source_duplicates(items: list[dict]) -> dict[str, int]:
    """Count how many sources report each unique event (title + date)."""
    event_counts = {}
    for item in items:
        key = (item.get("title", ""), item.get("date", ""))
        event_counts[key] = event_counts.get(key, 0) + 1
    return event_counts


# ==================== MAIN PIPELINE ====================

def run_pipeline():
    """Main pipeline execution."""
    print("Starting fake news pipeline...")

    # Step 1: Load raw data
    print("Loading raw data from output/raw/*/")
    raw_data = load_raw_data(RAW_DIR)
    print(f"Loaded {len(raw_data)} raw items")

    if not raw_data:
        print("No raw data found. Exiting.")
        return

    # Step 2: Clean data
    print("Cleaning data...")
    cleaned_data = []

    for item in raw_data:
        cleaned = {
            "title": clean_text(item.get("title", "")),
            "date": standardize_date(item.get("date", "")),
            "summary": clean_text(item.get("summary", "")),
            "url": clean_text(item.get("url", "")),
            "source": item.get("source", "unknown"),
        }
        cleaned_data.append(cleaned)

    # Step 3: Remove duplicates (same title + date)
    print("Removing duplicates...")
    seen = set()
    unique_data = []

    for item in cleaned_data:
        key = (item["title"], item["date"])
        if key not in seen:
            seen.add(key)
            unique_data.append(item)

    print(f"Removed {len(cleaned_data) - len(unique_data)} duplicates")

    # Step 4: Count source duplicates for trust boost
    event_counts = count_source_duplicates(unique_data)

    # Step 5: Verify news
    print("Verifying news...")
    afp_data = [item for item in unique_data if item["source"] == "afp"]

    processed_data = []
    for item in unique_data:
        # Get base verification score
        verification_score = get_verification_score(item, afp_data)

        # Boost score if multiple sources report same event
        key = (item["title"], item["date"])
        source_count = event_counts.get(key, 1)
        if source_count >= 2:
            verification_score = min(1.0, verification_score + 0.1 * (source_count - 1))

        # Determine if verified (score >= 0.7)
        is_verified = verification_score >= 0.7

        processed_item = {
            **item,
            "verification_score": verification_score,
            "is_verified": is_verified,
            "scrape_timestamp": datetime.now().isoformat(),
            "run_id": generate_run_id(),
        }
        processed_data.append(processed_item)

    print(f"Verified {sum(1 for i in processed_data if i['is_verified'])} items")

    # Step 6: Save processed data
    run_id = generate_run_id()
    processed_dir = Path(PROCESSED_DIR)
    processed_dir.mkdir(parents=True, exist_ok=True)

    # Convert to DataFrame and save as Parquet
    df = pd.DataFrame(processed_data)
    parquet_path = processed_dir / f"{run_id}.parquet"

    print(f"Saving processed data to {parquet_path}")
    df.to_parquet(parquet_path, index=False)

    # Also save as CSV for easy inspection
    csv_path = processed_dir / f"{run_id}.csv"
    df.to_csv(csv_path, index=False)

    print(f"Pipeline complete! Processed {len(processed_data)} items.")
    print(f"Saved to: {parquet_path}")


if __name__ == "__main__":
    run_pipeline()