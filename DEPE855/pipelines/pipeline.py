import json
import os
from datetime import datetime
from pathlib import Path
import hashlib
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from hdfs3 import HDFileSystem


# ==================== CONFIG ====================
# Use HDFS if configured, otherwise fall back to local filesystem
HDFS_HOST = os.getenv("HDFS_HOST", "")
HDFS_PORT = int(os.getenv("HDFS_PORT", "8020"))
USE_HDFS = bool(HDFS_HOST)

RAW_DIR = "output/raw"
PROCESSED_DIR = "output/processed"
FAKE_NEWS_API_KEY = os.getenv("FAKE_NEWS_API_KEY", "")
FAKE_NEWS_API_URL = "https://api.fakenewsdetector.com/v1/score"

# Initialize HDFS connection if configured
_hdfs = None
if USE_HDFS:
    _hdfs = HDFileSystem(host=HDFS_HOST, port=HDFS_PORT)


def _hdfs_exists(path: str) -> bool:
    """Check if path exists in HDFS."""
    if not USE_HDFS:
        return Path(path).exists()
    return _hdfs.exists(path)


def _hdfs_ls(path: str) -> list[str]:
    """List files in HDFS directory."""
    if not USE_HDFS:
        return [str(p) for p in Path(path).glob("**/*.json")]
    return _hdfs.ls(path, detail=False)


def _hdfs_read_text(path: str) -> str:
    """Read file from HDFS."""
    if not USE_HDFS:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    with _hdfs.open(path, "rb") as f:
        return f.read().decode("utf-8")


def _hdfs_write_parquet(df: pd.DataFrame, path: str):
    """Write DataFrame to Parquet in HDFS."""
    if not USE_HDFS:
        df.to_parquet(path, index=False)
    else:
        with _hdfs.open(path, "wb") as f:
            df.to_parquet(f, index=False)


def _hdfs_write_csv(df: pd.DataFrame, path: str):
    """Write DataFrame to CSV in HDFS."""
    if not USE_HDFS:
        df.to_csv(path, index=False)
    else:
        with _hdfs.open(path, "wb") as f:
            df.to_csv(f, index=False)


def _hdfs_mkdir(path: str):
    """Create directory in HDFS."""
    if not USE_HDFS:
        Path(path).mkdir(parents=True, exist_ok=True)
    else:
        _hdfs.makedirs(path)


# ==================== HELPERS ====================

def load_raw_data(raw_dir: str) -> list[dict]:
    """Load all JSON files from output/raw/*/ directories (local or HDFS)."""
    raw_data = []

    if not _hdfs_exists(raw_dir):
        print(f"Warning: {raw_dir} does not exist. Creating...")
        _hdfs_mkdir(raw_dir)
        return raw_data

    # Get all JSON files recursively
    all_files = _hdfs_ls(raw_dir)
    json_files = [f for f in all_files if f.endswith(".json")]

    for json_file in json_files:
        try:
            content = _hdfs_read_text(json_file)
            data = json.loads(content)
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
        return "1970-01-01"

    if date_str.count("-") == 2 and len(date_str) == 10:
        return date_str

    if "/" in date_str:
        parts = date_str.split("/")
        if len(parts) == 3:
            day, month, year = parts
            try:
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            except (ValueError, AttributeError):
                pass

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
    """Calculate verification score for an item."""
    base_score = 0.5

    afp_verified = False
    for afp_item in afp_data:
        if (afp_item.get("title") == item.get("title") and
                afp_item.get("date") == item.get("date")):
            afp_rating = afp_item.get("rating", "").lower()
            if "true" in afp_rating or "real" in afp_rating:
                base_score = 0.95
                afp_verified = True
            elif "false" in afp_rating or "fake" in afp_rating:
                base_score = 0.05
                afp_verified = True
            break

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

    verification_score = (base_score + fake_news_score) / 2
    if afp_verified:
        verification_score = base_score

    return round(verification_score, 2)


def count_source_duplicates(items: list[dict]) -> dict:
    """Count how many sources report each unique event."""
    event_counts = {}
    for item in items:
        key = (item.get("title", ""), item.get("date", ""))
        event_counts[key] = event_counts.get(key, 0) + 1
    return event_counts


# ==================== MAIN PIPELINE ====================

def run_pipeline():
    """Main pipeline execution with HDFS support."""
    print(f"Starting fake news pipeline... (HDFS: {'enabled' if USE_HDFS else 'disabled'})")

    # Step 1: Load raw data
    print(f"Loading raw data from {RAW_DIR}")
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

    # Step 3: Remove duplicates
    print("Removing duplicates...")
    seen = set()
    unique_data = []

    for item in cleaned_data:
        key = (item["title"], item["date"])
        if key not in seen:
            seen.add(key)
            unique_data.append(item)

    print(f"Removed {len(cleaned_data) - len(unique_data)} duplicates")

    # Step 4: Count source duplicates
    event_counts = count_source_duplicates(unique_data)

    # Step 5: Verify news
    print("Verifying news...")
    afp_data = [item for item in unique_data if item["source"] == "afp"]

    processed_data = []
    for item in unique_data:
        verification_score = get_verification_score(item, afp_data)
        key = (item["title"], item["date"])
        source_count = event_counts.get(key, 1)
        if source_count >= 2:
            verification_score = min(1.0, verification_score + 0.1 * (source_count - 1))

        processed_item = {
            **item,
            "verification_score": verification_score,
            "is_verified": verification_score >= 0.7,
            "scrape_timestamp": datetime.now().isoformat(),
            "run_id": generate_run_id(),
        }
        processed_data.append(processed_item)

    print(f"Verified {sum(1 for i in processed_data if i['is_verified'])} items")

    # Step 6: Save processed data
    run_id = generate_run_id()
    _hdfs_mkdir(PROCESSED_DIR)

    parquet_path = f"{PROCESSED_DIR}/{run_id}.parquet"
    csv_path = f"{PROCESSED_DIR}/{run_id}.csv"

    print(f"Saving processed data to {parquet_path}")
    df = pd.DataFrame(processed_data)
    _hdfs_write_parquet(df, parquet_path)
    _hdfs_write_csv(df, csv_path)

    print(f"Pipeline complete! Processed {len(processed_data)} items.")
    print(f"Saved to: {parquet_path}")


if __name__ == "__main__":
    run_pipeline()