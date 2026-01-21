"""
Fetch module for Podcast Breakdown Pipeline.

Retrieves YouTube transcripts and metadata using youtube-transcript-api.
No YouTube API key required.
"""

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from urllib.parse import parse_qs, urlparse

from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import (
    NoTranscriptFound,
    TranscriptsDisabled,
    VideoUnavailable,
)


@dataclass
class TranscriptSegment:
    """A single segment of the transcript with timing info."""
    text: str
    start: float  # Start time in seconds
    duration: float


@dataclass
class VideoMetadata:
    """Metadata about the YouTube video."""
    video_id: str
    url: str
    title: str
    channel: str
    # These fields require yt-dlp or YouTube API for accurate data
    # For now, we'll leave them optional and fill what we can
    channel_id: Optional[str] = None
    duration_seconds: Optional[int] = None
    duration_formatted: Optional[str] = None
    publish_date: Optional[str] = None
    fetched_at: Optional[str] = None


@dataclass
class FetchResult:
    """Complete result from fetching a video's transcript."""
    metadata: VideoMetadata
    segments: list[TranscriptSegment]
    full_text: str
    word_count: int


def extract_video_id(url: str) -> str:
    """
    Extract the video ID from various YouTube URL formats.

    Handles:
        - https://www.youtube.com/watch?v=VIDEO_ID
        - https://youtu.be/VIDEO_ID
        - https://www.youtube.com/embed/VIDEO_ID
        - https://www.youtube.com/v/VIDEO_ID

    Args:
        url: YouTube video URL

    Returns:
        The 11-character video ID

    Raises:
        ValueError: If URL format is not recognized or video ID not found
    """
    # Handle youtu.be short URLs
    if "youtu.be" in url:
        # Format: https://youtu.be/VIDEO_ID or https://youtu.be/VIDEO_ID?params
        parsed = urlparse(url)
        video_id = parsed.path.lstrip("/")
        # Remove any path segments after the ID
        video_id = video_id.split("/")[0]
        if video_id:
            return video_id

    # Handle standard youtube.com URLs
    parsed = urlparse(url)

    # Check for /watch?v= format
    if parsed.path == "/watch":
        query_params = parse_qs(parsed.query)
        if "v" in query_params:
            return query_params["v"][0]

    # Check for /embed/VIDEO_ID or /v/VIDEO_ID format
    path_patterns = ["/embed/", "/v/"]
    for pattern in path_patterns:
        if pattern in parsed.path:
            video_id = parsed.path.split(pattern)[1]
            # Remove any trailing path or query
            video_id = video_id.split("/")[0].split("?")[0]
            if video_id:
                return video_id

    # Try regex as fallback for edge cases
    # Matches 11-character video IDs
    match = re.search(r"(?:v=|/)([a-zA-Z0-9_-]{11})(?:[&?/]|$)", url)
    if match:
        return match.group(1)

    raise ValueError(f"Could not extract video ID from URL: {url}")


def fetch_transcript(video_id: str) -> tuple[list[TranscriptSegment], str]:
    """
    Fetch the transcript for a video.

    Args:
        video_id: YouTube video ID

    Returns:
        Tuple of (list of TranscriptSegment, full text string)

    Raises:
        ValueError: If transcript cannot be fetched
    """
    try:
        # Create API instance (required in newer versions of youtube-transcript-api)
        ytt_api = YouTubeTranscriptApi()

        # Try to get transcript, preferring manual captions over auto-generated
        transcript_list = ytt_api.list(video_id)

        # Try manual transcripts first (usually higher quality)
        try:
            transcript = transcript_list.find_manually_created_transcript(["en"])
        except NoTranscriptFound:
            # Fall back to auto-generated
            try:
                transcript = transcript_list.find_generated_transcript(["en"])
            except NoTranscriptFound:
                # Try any available transcript
                transcript = transcript_list.find_transcript(["en"])

        # Fetch the actual transcript data
        transcript_data = transcript.fetch()

    except TranscriptsDisabled:
        raise ValueError(f"Transcripts are disabled for video: {video_id}")
    except VideoUnavailable:
        raise ValueError(f"Video unavailable: {video_id}")
    except NoTranscriptFound:
        raise ValueError(f"No transcript found for video: {video_id}")

    # Convert to our dataclass format
    # transcript_data is now a FetchedTranscript object, iterate over it
    segments = [
        TranscriptSegment(
            text=snippet.text,
            start=snippet.start,
            duration=snippet.duration
        )
        for snippet in transcript_data
    ]

    # Build full text by joining all segments
    full_text = " ".join(seg.text for seg in segments)

    return segments, full_text


def fetch_metadata_basic(video_id: str, url: str) -> VideoMetadata:
    """
    Create basic metadata for a video.

    Note: Getting full metadata (title, channel, duration) requires either:
    - yt-dlp library (recommended, no API key needed)
    - YouTube Data API (requires API key)

    For now, this returns a minimal metadata object. We'll enhance this
    with yt-dlp in a future iteration.

    Args:
        video_id: YouTube video ID
        url: Original URL

    Returns:
        VideoMetadata with basic info filled in
    """
    return VideoMetadata(
        video_id=video_id,
        url=url,
        title=f"Video {video_id}",  # Placeholder - will be updated
        channel="Unknown",  # Placeholder - will be updated
        fetched_at=datetime.utcnow().isoformat() + "Z"
    )


def fetch_metadata_ytdlp(video_id: str, url: str) -> VideoMetadata:
    """
    Fetch full metadata using yt-dlp (no API key required).

    Args:
        video_id: YouTube video ID
        url: Original URL

    Returns:
        VideoMetadata with all available fields

    Raises:
        ImportError: If yt-dlp is not installed
        ValueError: If metadata cannot be fetched
    """
    try:
        import yt_dlp
    except ImportError:
        raise ImportError(
            "yt-dlp is required for full metadata. "
            "Install with: pip install yt-dlp"
        )

    # Configure yt-dlp to only extract info, not download
    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": False,
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        try:
            info = ydl.extract_info(url, download=False)
        except Exception as e:
            raise ValueError(f"Failed to fetch metadata: {e}")

    # Format duration
    duration_secs = info.get("duration")
    duration_fmt = None
    if duration_secs:
        hours, remainder = divmod(duration_secs, 3600)
        minutes, seconds = divmod(remainder, 60)
        if hours:
            duration_fmt = f"{int(hours)}:{int(minutes):02d}:{int(seconds):02d}"
        else:
            duration_fmt = f"{int(minutes)}:{int(seconds):02d}"

    # Parse upload date (format: YYYYMMDD)
    upload_date = info.get("upload_date")
    publish_date = None
    if upload_date and len(upload_date) == 8:
        publish_date = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:]}"

    return VideoMetadata(
        video_id=video_id,
        url=url,
        title=info.get("title", f"Video {video_id}"),
        channel=info.get("channel", info.get("uploader", "Unknown")),
        channel_id=info.get("channel_id"),
        duration_seconds=duration_secs,
        duration_formatted=duration_fmt,
        publish_date=publish_date,
        fetched_at=datetime.utcnow().isoformat() + "Z"
    )


def fetch_video(url: str, use_ytdlp: bool = True) -> FetchResult:
    """
    Fetch transcript and metadata for a YouTube video.

    This is the main entry point for the fetch module.

    Args:
        url: YouTube video URL
        use_ytdlp: If True, use yt-dlp for full metadata. If False, use basic metadata.

    Returns:
        FetchResult with metadata, transcript segments, and full text

    Raises:
        ValueError: If video ID cannot be extracted or transcript unavailable
    """
    # Extract video ID from URL
    video_id = extract_video_id(url)

    # Fetch transcript
    segments, full_text = fetch_transcript(video_id)

    # Fetch metadata
    if use_ytdlp:
        try:
            metadata = fetch_metadata_ytdlp(video_id, url)
        except ImportError:
            # Fall back to basic if yt-dlp not installed
            print("Warning: yt-dlp not installed, using basic metadata")
            metadata = fetch_metadata_basic(video_id, url)
    else:
        metadata = fetch_metadata_basic(video_id, url)

    return FetchResult(
        metadata=metadata,
        segments=segments,
        full_text=full_text,
        word_count=len(full_text.split())
    )


# === CLI for testing ===
if __name__ == "__main__":
    import sys

    # Fix Windows encoding issues
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

    if len(sys.argv) < 2:
        print("Usage: python fetch.py <youtube_url>")
        sys.exit(1)

    url = sys.argv[1]
    print(f"Fetching: {url}\n")

    try:
        result = fetch_video(url)

        print(f"Title: {result.metadata.title}")
        print(f"Channel: {result.metadata.channel}")
        print(f"Duration: {result.metadata.duration_formatted or 'Unknown'}")
        print(f"Published: {result.metadata.publish_date or 'Unknown'}")
        print(f"Segments: {len(result.segments)}")
        print(f"Words: {result.word_count}")
        print(f"\nFirst 500 chars of transcript:")
        print(result.full_text[:500] + "...")

    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)
