"""
Storage module for Podcast Breakdown Pipeline.

Handles folder creation, file writing, and organization of processed podcasts.
"""

import json
import re
import unicodedata
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

from config import FOLDER_DATE_FORMAT, MAX_SLUG_LENGTH, PODCASTS_DIR
from fetch import FetchResult, VideoMetadata


def slugify(text: str, max_length: int = MAX_SLUG_LENGTH) -> str:
    """
    Convert text to a URL/filesystem-safe slug.

    Args:
        text: Text to convert
        max_length: Maximum length of resulting slug

    Returns:
        Lowercase slug with only alphanumeric chars and hyphens

    Examples:
        >>> slugify("Sam Altman: OpenAI & GPT-5!")
        'sam-altman-openai-gpt-5'
        >>> slugify("Lex Fridman Podcast")
        'lex-fridman-podcast'
    """
    # Normalize unicode characters
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")

    # Convert to lowercase
    text = text.lower()

    # Replace common separators with hyphens
    text = re.sub(r"[:\|/\\&]+", "-", text)

    # Replace spaces and underscores with hyphens
    text = re.sub(r"[\s_]+", "-", text)

    # Remove any character that isn't alphanumeric or hyphen
    text = re.sub(r"[^a-z0-9-]", "", text)

    # Collapse multiple hyphens into one
    text = re.sub(r"-+", "-", text)

    # Strip leading/trailing hyphens
    text = text.strip("-")

    # Truncate to max length, but don't cut in middle of word
    if len(text) > max_length:
        text = text[:max_length]
        # If we cut in the middle of a word, back up to last hyphen
        if "-" in text:
            text = text.rsplit("-", 1)[0]

    return text


def generate_folder_name(metadata: VideoMetadata) -> str:
    """
    Generate a folder name from video metadata.

    Format: {date}_{channel-slug}_{title-slug}
    Example: 2025-01-10_lex-fridman_sam-altman-openai

    Args:
        metadata: VideoMetadata object

    Returns:
        Folder name string
    """
    # Use publish date if available, otherwise fetch date
    if metadata.publish_date:
        date_str = metadata.publish_date
    elif metadata.fetched_at:
        # Parse ISO format and reformat
        dt = datetime.fromisoformat(metadata.fetched_at.replace("Z", "+00:00"))
        date_str = dt.strftime(FOLDER_DATE_FORMAT)
    else:
        date_str = datetime.now().strftime(FOLDER_DATE_FORMAT)

    # Create slugs for channel and title
    channel_slug = slugify(metadata.channel, max_length=30)
    title_slug = slugify(metadata.title, max_length=40)

    return f"{date_str}_{channel_slug}_{title_slug}"


def create_podcast_folder(
    fetch_result: FetchResult,
    base_dir: Optional[Path] = None
) -> Path:
    """
    Create the folder structure for a processed podcast.

    Creates:
        {base_dir}/{folder_name}/
        {base_dir}/{folder_name}/analyses/

    Args:
        fetch_result: FetchResult from fetch module
        base_dir: Base directory for podcasts (default: PODCASTS_DIR from config)

    Returns:
        Path to the created podcast folder
    """
    base_dir = base_dir or PODCASTS_DIR

    # Generate folder name
    folder_name = generate_folder_name(fetch_result.metadata)
    podcast_dir = base_dir / folder_name

    # Create directories
    podcast_dir.mkdir(parents=True, exist_ok=True)
    (podcast_dir / "analyses").mkdir(exist_ok=True)

    return podcast_dir


def save_metadata(metadata: VideoMetadata, podcast_dir: Path) -> Path:
    """
    Save video metadata to metadata.json.

    Args:
        metadata: VideoMetadata object
        podcast_dir: Path to podcast folder

    Returns:
        Path to saved metadata file
    """
    metadata_path = podcast_dir / "metadata.json"

    # Convert dataclass to dict
    metadata_dict = asdict(metadata)

    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata_dict, f, indent=2, ensure_ascii=False)

    return metadata_path


def save_transcript(
    fetch_result: FetchResult,
    podcast_dir: Path
) -> tuple[Path, Path]:
    """
    Save transcript in both JSON (with timestamps) and plain text formats.

    Args:
        fetch_result: FetchResult from fetch module
        podcast_dir: Path to podcast folder

    Returns:
        Tuple of (json_path, txt_path)
    """
    # Save JSON with full segment data
    json_path = podcast_dir / "transcript.json"
    transcript_data = {
        "segments": [
            {
                "text": seg.text,
                "start": seg.start,
                "duration": seg.duration
            }
            for seg in fetch_result.segments
        ],
        "full_text": fetch_result.full_text,
        "word_count": fetch_result.word_count,
        "segment_count": len(fetch_result.segments)
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(transcript_data, f, indent=2, ensure_ascii=False)

    # Save plain text for easy reading
    txt_path = podcast_dir / "transcript.txt"
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(fetch_result.full_text)

    return json_path, txt_path


def save_analysis(
    content: str,
    analysis_name: str,
    podcast_dir: Path,
    prompt_file: Optional[str] = None,
    model: Optional[str] = None
) -> Path:
    """
    Save an analysis output to the analyses folder.

    Adds a header with generation metadata.

    Args:
        content: The analysis content (markdown)
        analysis_name: Name of the analysis (e.g., "summary", "key_claims")
        podcast_dir: Path to podcast folder
        prompt_file: Name of the prompt file used (optional)
        model: Model name used for generation (optional)

    Returns:
        Path to saved analysis file
    """
    analyses_dir = podcast_dir / "analyses"
    analyses_dir.mkdir(exist_ok=True)

    # Slugify the analysis name for filename
    filename = slugify(analysis_name) + ".md"
    analysis_path = analyses_dir / filename

    # Build header
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    header_lines = [
        f"# {analysis_name.replace('_', ' ').title()}",
        "",
        f"> Generated: {timestamp}",
    ]
    if model:
        header_lines.append(f"> Model: {model}")
    if prompt_file:
        header_lines.append(f"> Prompt: {prompt_file}")
    header_lines.extend(["", "---", ""])

    header = "\n".join(header_lines)

    # Write file
    with open(analysis_path, "w", encoding="utf-8") as f:
        f.write(header + content)

    return analysis_path


def save_fetch_result(
    fetch_result: FetchResult,
    base_dir: Optional[Path] = None
) -> Path:
    """
    Save all data from a fetch result to disk.

    This is a convenience function that creates the folder and saves
    metadata + transcript in one call.

    Args:
        fetch_result: FetchResult from fetch module
        base_dir: Base directory for podcasts (default: PODCASTS_DIR)

    Returns:
        Path to the created podcast folder
    """
    # Create folder structure
    podcast_dir = create_podcast_folder(fetch_result, base_dir)

    # Save metadata
    save_metadata(fetch_result.metadata, podcast_dir)

    # Save transcript
    save_transcript(fetch_result, podcast_dir)

    return podcast_dir


def list_podcasts(base_dir: Optional[Path] = None) -> list[dict]:
    """
    List all processed podcasts.

    Args:
        base_dir: Base directory to scan (default: PODCASTS_DIR)

    Returns:
        List of dicts with podcast info, sorted by date (newest first)
    """
    base_dir = base_dir or PODCASTS_DIR

    if not base_dir.exists():
        return []

    podcasts = []

    for folder in base_dir.iterdir():
        if not folder.is_dir():
            continue

        metadata_path = folder / "metadata.json"
        if not metadata_path.exists():
            continue

        with open(metadata_path, "r", encoding="utf-8") as f:
            metadata = json.load(f)

        # List available analyses
        analyses_dir = folder / "analyses"
        analyses = []
        if analyses_dir.exists():
            analyses = [p.stem for p in analyses_dir.glob("*.md")]

        podcasts.append({
            "folder": folder.name,
            "path": str(folder),
            "metadata": metadata,
            "analyses": analyses
        })

    # Sort by publish date or folder name (both start with date)
    podcasts.sort(key=lambda p: p["folder"], reverse=True)

    return podcasts


def get_podcast(folder_name: str, base_dir: Optional[Path] = None) -> Optional[dict]:
    """
    Get full data for a specific podcast.

    Args:
        folder_name: Name of the podcast folder
        base_dir: Base directory (default: PODCASTS_DIR)

    Returns:
        Dict with metadata, transcript, and analyses, or None if not found
    """
    base_dir = base_dir or PODCASTS_DIR
    podcast_dir = base_dir / folder_name

    if not podcast_dir.exists():
        return None

    result = {
        "folder": folder_name,
        "path": str(podcast_dir),
    }

    # Load metadata
    metadata_path = podcast_dir / "metadata.json"
    if metadata_path.exists():
        with open(metadata_path, "r", encoding="utf-8") as f:
            result["metadata"] = json.load(f)

    # Load transcript
    transcript_json = podcast_dir / "transcript.json"
    if transcript_json.exists():
        with open(transcript_json, "r", encoding="utf-8") as f:
            result["transcript"] = json.load(f)

    # Load analyses
    analyses_dir = podcast_dir / "analyses"
    result["analyses"] = {}
    if analyses_dir.exists():
        for analysis_file in analyses_dir.glob("*.md"):
            with open(analysis_file, "r", encoding="utf-8") as f:
                result["analyses"][analysis_file.stem] = f.read()

    return result


# === CLI for testing ===
if __name__ == "__main__":
    # Test slugify
    print("Testing slugify:")
    test_cases = [
        "Sam Altman: OpenAI & GPT-5!",
        "Lex Fridman Podcast",
        "The FUTURE of AI -- what's next???",
        "日本語テスト (Japanese Test)",
    ]
    for test in test_cases:
        print(f"  '{test}' -> '{slugify(test)}'")

    print("\nListing podcasts:")
    podcasts = list_podcasts()
    if podcasts:
        for p in podcasts:
            print(f"  - {p['folder']}")
    else:
        print("  (no podcasts found)")
