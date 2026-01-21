"""
Full-text search module for Podcast Breakdown Pipeline.

Uses SQLite FTS5 for fast, free, local full-text search across all podcast content.
Indexes: title, channel, transcript, and all analysis outputs.
"""

import json
import sqlite3
from pathlib import Path
from typing import Optional

from config import BASE_DIR, PODCASTS_DIR


# Database file location
SEARCH_DB = BASE_DIR / "search_index.db"


def get_connection() -> sqlite3.Connection:
    """
    Get a connection to the search database.
    Creates the database and tables if they don't exist.
    """
    conn = sqlite3.connect(SEARCH_DB)
    conn.row_factory = sqlite3.Row  # Enable dict-like access

    # Create FTS5 virtual table if it doesn't exist
    # FTS5 handles tokenization, stemming, and ranking automatically
    conn.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS podcasts_fts USING fts5(
            folder,           -- folder name (primary key equivalent)
            title,            -- video title
            channel,          -- channel name
            duration_seconds, -- for filtering/sorting (stored as text in FTS)
            publish_date,     -- for filtering/sorting
            transcript,       -- full transcript text
            analyses,         -- concatenated analysis content
            tokenize='porter unicode61'  -- Porter stemmer + unicode support
        )
    """)

    # Create a regular table for metadata that doesn't need FTS
    conn.execute("""
        CREATE TABLE IF NOT EXISTS podcast_meta (
            folder TEXT PRIMARY KEY,
            video_id TEXT,
            url TEXT,
            channel_id TEXT,
            duration_formatted TEXT,
            fetched_at TEXT,
            analysis_names TEXT  -- JSON array of analysis names
        )
    """)

    conn.commit()
    return conn


def index_podcast(folder_path: Path, conn: Optional[sqlite3.Connection] = None) -> bool:
    """
    Index a single podcast folder into the search database.

    Args:
        folder_path: Path to the podcast folder
        conn: Optional existing connection (will create one if not provided)

    Returns:
        True if indexed successfully, False otherwise
    """
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True

    try:
        folder = folder_path.name

        # Load metadata
        metadata_path = folder_path / "metadata.json"
        if not metadata_path.exists():
            return False

        with open(metadata_path, "r", encoding="utf-8") as f:
            metadata = json.load(f)

        # Load transcript
        transcript = ""
        transcript_path = folder_path / "transcript.txt"
        if transcript_path.exists():
            with open(transcript_path, "r", encoding="utf-8") as f:
                transcript = f.read()

        # Load and concatenate all analyses
        analyses_content = []
        analysis_names = []
        analyses_dir = folder_path / "analyses"
        if analyses_dir.exists():
            for analysis_file in analyses_dir.glob("*.md"):
                analysis_names.append(analysis_file.stem)
                with open(analysis_file, "r", encoding="utf-8") as f:
                    # Include the analysis name as a header for searchability
                    content = f.read()
                    analyses_content.append(f"=== {analysis_file.stem} ===\n{content}")

        analyses_text = "\n\n".join(analyses_content)

        # Delete existing entry if present (for re-indexing)
        conn.execute("DELETE FROM podcasts_fts WHERE folder = ?", (folder,))
        conn.execute("DELETE FROM podcast_meta WHERE folder = ?", (folder,))

        # Insert into FTS table
        conn.execute("""
            INSERT INTO podcasts_fts (folder, title, channel, duration_seconds, publish_date, transcript, analyses)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            folder,
            metadata.get("title", ""),
            metadata.get("channel", ""),
            str(metadata.get("duration_seconds", 0)),
            metadata.get("publish_date", ""),
            transcript,
            analyses_text
        ))

        # Insert into metadata table
        conn.execute("""
            INSERT INTO podcast_meta (folder, video_id, url, channel_id, duration_formatted, fetched_at, analysis_names)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            folder,
            metadata.get("video_id", ""),
            metadata.get("url", ""),
            metadata.get("channel_id", ""),
            metadata.get("duration_formatted", ""),
            metadata.get("fetched_at", ""),
            json.dumps(analysis_names)
        ))

        conn.commit()
        return True

    except Exception as e:
        print(f"Error indexing {folder_path}: {e}")
        return False

    finally:
        if close_conn:
            conn.close()


def build_full_index(podcasts_dir: Optional[Path] = None) -> dict:
    """
    Build or rebuild the full search index from all podcasts on disk.

    Args:
        podcasts_dir: Directory containing podcast folders (default: PODCASTS_DIR)

    Returns:
        Dict with 'indexed' count and 'errors' list
    """
    podcasts_dir = podcasts_dir or PODCASTS_DIR

    if not podcasts_dir.exists():
        return {"indexed": 0, "errors": ["Podcasts directory does not exist"]}

    conn = get_connection()
    indexed = 0
    errors = []

    try:
        for folder in podcasts_dir.iterdir():
            if not folder.is_dir():
                continue

            if index_podcast(folder, conn):
                indexed += 1
                print(f"  Indexed: {folder.name}")
            else:
                errors.append(folder.name)
                print(f"  Skipped: {folder.name}")

        return {"indexed": indexed, "errors": errors}

    finally:
        conn.close()


def search(
    query: str,
    limit: int = 50,
    channel: Optional[str] = None,
    min_duration: Optional[int] = None,
    max_duration: Optional[int] = None
) -> list[dict]:
    """
    Search the podcast index.

    Args:
        query: Search query (supports FTS5 syntax: AND, OR, NOT, "phrases", prefix*)
        limit: Maximum number of results
        channel: Filter to specific channel name
        min_duration: Minimum duration in seconds
        max_duration: Maximum duration in seconds

    Returns:
        List of matching podcasts with snippets and rank scores
    """
    conn = get_connection()

    try:
        # Build the query
        # FTS5 MATCH handles the full-text search
        # We use highlight() and snippet() for result excerpts

        # Base query with search
        sql = """
            SELECT
                fts.folder,
                fts.title,
                fts.channel,
                fts.duration_seconds,
                fts.publish_date,
                meta.url,
                meta.duration_formatted,
                meta.analysis_names,
                bm25(podcasts_fts) as rank,
                snippet(podcasts_fts, 5, '<mark>', '</mark>', '...', 32) as transcript_snippet,
                snippet(podcasts_fts, 6, '<mark>', '</mark>', '...', 32) as analysis_snippet
            FROM podcasts_fts fts
            JOIN podcast_meta meta ON fts.folder = meta.folder
            WHERE podcasts_fts MATCH ?
        """

        params = [query]

        # Add filters
        if channel:
            sql += " AND fts.channel = ?"
            params.append(channel)

        if min_duration is not None:
            sql += " AND CAST(fts.duration_seconds AS INTEGER) >= ?"
            params.append(min_duration)

        if max_duration is not None:
            sql += " AND CAST(fts.duration_seconds AS INTEGER) <= ?"
            params.append(max_duration)

        # Order by relevance (bm25 returns negative values, more negative = more relevant)
        sql += " ORDER BY rank LIMIT ?"
        params.append(limit)

        cursor = conn.execute(sql, params)
        rows = cursor.fetchall()

        results = []
        for row in rows:
            results.append({
                "folder": row["folder"],
                "title": row["title"],
                "channel": row["channel"],
                "duration_seconds": int(row["duration_seconds"]) if row["duration_seconds"] else 0,
                "duration_formatted": row["duration_formatted"],
                "publish_date": row["publish_date"],
                "url": row["url"],
                "analyses": json.loads(row["analysis_names"]) if row["analysis_names"] else [],
                "rank": row["rank"],
                "transcript_snippet": row["transcript_snippet"],
                "analysis_snippet": row["analysis_snippet"]
            })

        return results

    finally:
        conn.close()


def get_channels() -> list[dict]:
    """
    Get list of all channels with podcast counts.

    Returns:
        List of dicts with 'channel' and 'count', sorted by count descending
    """
    conn = get_connection()

    try:
        cursor = conn.execute("""
            SELECT channel, COUNT(*) as count
            FROM podcasts_fts
            GROUP BY channel
            ORDER BY count DESC, channel ASC
        """)

        return [{"channel": row["channel"], "count": row["count"]} for row in cursor.fetchall()]

    finally:
        conn.close()


def get_index_stats() -> dict:
    """
    Get statistics about the search index.

    Returns:
        Dict with total count, channel breakdown, etc.
    """
    conn = get_connection()

    try:
        # Total podcasts indexed
        total = conn.execute("SELECT COUNT(*) FROM podcasts_fts").fetchone()[0]

        # Channels
        channels = get_channels()

        # Duration range
        duration_stats = conn.execute("""
            SELECT
                MIN(CAST(duration_seconds AS INTEGER)) as min_duration,
                MAX(CAST(duration_seconds AS INTEGER)) as max_duration,
                AVG(CAST(duration_seconds AS INTEGER)) as avg_duration
            FROM podcasts_fts
        """).fetchone()

        return {
            "total_podcasts": total,
            "channels": channels,
            "duration": {
                "min_seconds": duration_stats["min_duration"] or 0,
                "max_seconds": duration_stats["max_duration"] or 0,
                "avg_seconds": int(duration_stats["avg_duration"] or 0)
            }
        }

    finally:
        conn.close()


# === CLI for testing ===
if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        command = sys.argv[1]

        if command == "build":
            print("Building full search index...")
            result = build_full_index()
            print(f"\nIndexed {result['indexed']} podcasts")
            if result['errors']:
                print(f"Errors: {result['errors']}")

        elif command == "search" and len(sys.argv) > 2:
            query = " ".join(sys.argv[2:])
            print(f"Searching for: {query}\n")
            results = search(query)

            if not results:
                print("No results found.")
            else:
                for i, r in enumerate(results, 1):
                    print(f"{i}. {r['title']}")
                    print(f"   Channel: {r['channel']} | Duration: {r['duration_formatted']}")
                    if r['transcript_snippet']:
                        print(f"   Transcript: {r['transcript_snippet'][:200]}...")
                    if r['analysis_snippet']:
                        print(f"   Analysis: {r['analysis_snippet'][:200]}...")
                    print()

        elif command == "stats":
            stats = get_index_stats()
            print(f"Total podcasts: {stats['total_podcasts']}")
            print(f"\nChannels:")
            for ch in stats['channels']:
                print(f"  {ch['channel']}: {ch['count']}")
            print(f"\nDuration range: {stats['duration']['min_seconds']//60}m - {stats['duration']['max_seconds']//60}m")
            print(f"Average duration: {stats['duration']['avg_seconds']//60}m")

        else:
            print("Usage:")
            print("  python search.py build           # Build index from all podcasts")
            print("  python search.py search <query>  # Search the index")
            print("  python search.py stats           # Show index statistics")
    else:
        print("Usage:")
        print("  python search.py build           # Build index from all podcasts")
        print("  python search.py search <query>  # Search the index")
        print("  python search.py stats           # Show index statistics")
