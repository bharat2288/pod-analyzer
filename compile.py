"""
EPUB compiler for Pod Transcriber.

Compiles processed podcasts into an EPUB book with chapters, suitable for
import into Scholia Reader or any EPUB reader. Each podcast becomes a chapter
containing its analyses (summary, concept map, etc.) and optionally the
full transcript.

Usage:
    python compile.py --channel "MIT OpenCourseWare" \
        --title "MIT 15.773: Hands-on Deep Learning" \
        --author "MIT OpenCourseWare" \
        --output "MIT-15773-Deep-Learning.epub"
"""

import argparse
import re
import sys
from typing import Optional

import markdown
from ebooklib import epub

from storage import list_podcasts, get_podcast


# ── XHTML template for chapter content ──────────────────────────────────
XHTML_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
<head>
  <meta charset="utf-8"/>
  <title>{title}</title>
  <style>
    body {{ font-family: Georgia, serif; line-height: 1.6; margin: 2em; }}
    h1 {{ font-size: 1.6em; margin-top: 1em; }}
    h2 {{ font-size: 1.2em; margin-top: 1.5em; color: #555; }}
    p.duration {{ font-style: italic; color: #888; margin-top: -0.5em; }}
    p {{ margin: 0.8em 0; }}
    ul, ol {{ margin: 0.8em 0; padding-left: 1.5em; }}
    code {{ background: #f4f4f4; padding: 0.1em 0.3em; font-size: 0.9em; }}
    pre {{ background: #f4f4f4; padding: 1em; overflow-x: auto; }}
    blockquote {{ border-left: 3px solid #ccc; margin: 1em 0; padding-left: 1em; color: #666; }}
    table {{ border-collapse: collapse; margin: 1em 0; }}
    th, td {{ border: 1px solid #ccc; padding: 0.4em 0.8em; }}
  </style>
</head>
<body>
{body}
</body>
</html>"""


# ── Filtering ────────────────────────────────────────────────────────────

def filter_by_channel(podcasts: list[dict], channel: str) -> list[dict]:
    """Filter podcasts by exact channel name (case-insensitive)."""
    channel_lower = channel.lower()
    return [
        p for p in podcasts
        if p.get("metadata", {}).get("channel", "").lower() == channel_lower
    ]


def filter_by_pattern(podcasts: list[dict], pattern: str) -> list[dict]:
    """Filter podcasts whose folder name contains the pattern."""
    pattern_lower = pattern.lower()
    return [
        p for p in podcasts
        if pattern_lower in p["folder"].lower()
    ]


def filter_by_folders(podcasts: list[dict], folders: list[str]) -> list[dict]:
    """Filter podcasts by explicit folder names."""
    folder_set = set(folders)
    return [p for p in podcasts if p["folder"] in folder_set]


# ── Sorting ──────────────────────────────────────────────────────────────

def lecture_sort_key(podcast: dict) -> tuple:
    """
    Extract a natural sort key from the podcast title slug.

    Folder format: {date}_{channel}_{title-slug}
    Title slug often starts with a number (e.g., "3-deep-learning-for-computer-vision").
    We extract the leading integer for numeric sorting so lecture 2 comes before 10.

    Returns:
        Tuple of (leading_number, full_folder_name) for stable sorting.
    """
    folder = podcast["folder"]
    # Title slug is the third segment after date and channel
    parts = folder.split("_", 2)
    title_slug = parts[2] if len(parts) > 2 else folder

    # Try to extract leading integer from title slug
    match = re.match(r"^(\d+)", title_slug)
    if match:
        return (int(match.group(1)), folder)

    # No leading number — sort alphabetically after numbered entries
    return (float("inf"), folder)


def sort_lectures(podcasts: list[dict]) -> list[dict]:
    """Sort podcasts by natural lecture order."""
    return sorted(podcasts, key=lecture_sort_key)


# ── Content transforms ──────────────────────────────────────────────────

def strip_header(content: str) -> str:
    """
    Strip the metadata header from an analysis file.

    Analysis files have a header (title, generated timestamp, model info)
    separated from the LLM content by a line of "---". We want just
    the content after that divider.
    """
    # Normalize Windows line endings
    content = content.replace("\r\n", "\n")

    # Split on the --- divider and take everything after it
    if "\n---\n" in content:
        _, after = content.split("\n---\n", 1)
        return after.strip()

    # No divider found — return as-is (shouldn't happen normally)
    return content.strip()


def demote_headings(html: str) -> str:
    """
    Replace heading tags (h1-h6) with styled <p> elements.

    EPUB readers like Scholia build their TOC by scanning for heading tags
    in the content. We only want our chapter h1 and section h2 to appear
    in the TOC — not every sub-heading inside the analysis markdown.
    This converts h1-h6 to styled paragraphs so they look right but
    don't pollute the navigation.
    """
    # Map heading levels to font sizes and weights
    styles = {
        "h1": "font-size:1.4em; font-weight:700; margin:1em 0 0.4em;",
        "h2": "font-size:1.2em; font-weight:700; margin:0.9em 0 0.3em;",
        "h3": "font-size:1.05em; font-weight:700; margin:0.8em 0 0.3em;",
        "h4": "font-size:1em; font-weight:700; margin:0.7em 0 0.2em;",
        "h5": "font-size:0.95em; font-weight:700; margin:0.6em 0 0.2em;",
        "h6": "font-size:0.9em; font-weight:700; margin:0.5em 0 0.2em;",
    }
    for tag, style in styles.items():
        # Replace opening tags (with or without attributes)
        html = re.sub(
            rf"<{tag}(\s[^>]*)?>",
            f'<p style="{style}">',
            html,
            flags=re.IGNORECASE,
        )
        html = re.sub(rf"</{tag}>", "</p>", html, flags=re.IGNORECASE)
    return html


def analysis_to_html(content: str) -> str:
    """Convert an analysis markdown file to HTML, stripping the header."""
    body = strip_header(content)
    # Convert markdown to HTML with extras (tables, fenced code, etc.)
    html = markdown.markdown(body, extensions=["extra"])
    # Replace headings with styled <p> so they don't appear in EPUB TOC
    return demote_headings(html)


def transcript_to_html(text: str) -> str:
    """
    Convert plain transcript text to HTML paragraphs.

    Groups sentences into paragraphs of ~5 sentences each for readability,
    since raw transcripts are usually one long block of text.
    """
    text = text.replace("\r\n", "\n").strip()

    # Split into sentences (period/question/exclamation followed by space)
    sentences = re.split(r"(?<=[.!?])\s+", text)

    # Group into paragraphs of ~5 sentences
    paragraphs = []
    group_size = 5
    for i in range(0, len(sentences), group_size):
        chunk = " ".join(sentences[i : i + group_size])
        paragraphs.append(f"<p>{chunk}</p>")

    return "\n".join(paragraphs)


# ── EPUB builders ────────────────────────────────────────────────────────

def format_duration(seconds: Optional[int]) -> str:
    """Format seconds into H:MM:SS or M:SS."""
    if not seconds:
        return ""
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    if hours > 0:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes}:{secs:02d}"


def build_intro_html(
    title: str,
    author: str,
    chapters: list[dict],
) -> str:
    """Build the introduction/cover page HTML."""
    # Calculate total duration
    total_seconds = sum(
        ch.get("metadata", {}).get("duration_seconds", 0) for ch in chapters
    )
    total_duration = format_duration(total_seconds)

    # Build chapter listing
    chapter_list = []
    for i, ch in enumerate(chapters, 1):
        ch_title = ch.get("metadata", {}).get("title", ch["folder"])
        dur = format_duration(ch.get("metadata", {}).get("duration_seconds", 0))
        dur_text = f" ({dur})" if dur else ""
        chapter_list.append(f"<li>{ch_title}{dur_text}</li>")

    body = f"""
<h1>{title}</h1>
<p><strong>{author}</strong></p>
<p>{len(chapters)} chapters &middot; {total_duration} total</p>
<h2>Contents</h2>
<ol>
{"".join(chapter_list)}
</ol>
"""
    return XHTML_TEMPLATE.format(title=title, body=body)


def build_chapter_html(
    chapter_num: int,
    podcast_data: dict,
    analysis_names: list[str],
    include_transcript: bool,
) -> tuple[str, list[tuple[str, str]]]:
    """
    Build the XHTML content for a single chapter.

    Args:
        chapter_num: Chapter number (1-based)
        podcast_data: Full podcast data from get_podcast()
        analysis_names: Which analyses to include (e.g., ["summary", "concept-map"])
        include_transcript: Whether to include the transcript section

    Returns:
        Tuple of (xhtml_string, list_of_subsection_tuples) where each
        subsection tuple is (anchor_id, display_name).
    """
    metadata = podcast_data.get("metadata", {})
    title = metadata.get("title", podcast_data["folder"])
    duration = format_duration(metadata.get("duration_seconds", 0))

    # Chapter heading
    ch_id = f"ch{chapter_num}"
    body_parts = [f'<h1 id="{ch_id}">{chapter_num}: {title}</h1>']
    if duration:
        body_parts.append(f'<p class="duration">{duration}</p>')

    subsections = []

    # Add each requested analysis
    analyses = podcast_data.get("analyses", {})
    for name in analysis_names:
        if name not in analyses:
            continue  # Silently skip missing analyses

        display_name = name.replace("-", " ").replace("_", " ").title()
        anchor = f"{ch_id}-{name}"
        body_parts.append(f'<h2 id="{anchor}">{display_name}</h2>')
        body_parts.append(analysis_to_html(analyses[name]))
        subsections.append((anchor, display_name))

    # Add transcript if requested
    if include_transcript:
        transcript_data = podcast_data.get("transcript", {})
        full_text = transcript_data.get("full_text", "")
        if full_text:
            anchor = f"{ch_id}-transcript"
            body_parts.append(f'<h2 id="{anchor}">Transcript</h2>')
            body_parts.append(transcript_to_html(full_text))
            subsections.append((anchor, "Transcript"))

    body = "\n".join(body_parts)
    xhtml = XHTML_TEMPLATE.format(title=f"Ch. {chapter_num}: {title}", body=body)
    return xhtml, subsections


def build_epub(
    title: str,
    author: str,
    chapters: list[dict],
    analysis_names: list[str],
    include_transcript: bool,
    output_path: str,
) -> None:
    """
    Assemble and write the complete EPUB file.

    Args:
        title: Book title
        author: Book author
        chapters: Sorted list of podcast dicts (with full data loaded)
        analysis_names: Which analyses to include per chapter
        include_transcript: Whether to include transcripts
        output_path: Where to write the EPUB file
    """
    book = epub.EpubBook()

    # Set metadata
    book.set_identifier(f"pod-transcriber-{title.lower().replace(' ', '-')[:40]}")
    book.set_title(title)
    book.set_language("en")
    book.add_author(author)

    # Build intro page
    intro_html = build_intro_html(title, author, chapters)
    intro_item = epub.EpubHtml(
        title="Introduction",
        file_name="intro.xhtml",
        lang="en",
    )
    intro_item.set_content(intro_html.encode("utf-8"))
    book.add_item(intro_item)

    # Build chapter pages
    chapter_items = []
    toc_entries = []

    for i, ch_data in enumerate(chapters, 1):
        ch_xhtml, subsections = build_chapter_html(
            chapter_num=i,
            podcast_data=ch_data,
            analysis_names=analysis_names,
            include_transcript=include_transcript,
        )

        ch_title = ch_data.get("metadata", {}).get("title", ch_data["folder"])
        file_name = f"chapter_{i:02d}.xhtml"

        ch_item = epub.EpubHtml(
            title=f"{i}: {ch_title}",
            file_name=file_name,
            lang="en",
        )
        ch_item.set_content(ch_xhtml.encode("utf-8"))
        book.add_item(ch_item)
        chapter_items.append(ch_item)

        # Chapter-level TOC entry only (sub-sections stay as h2 in content)
        toc_entries.append(ch_item)

    # Set table of contents
    book.toc = [intro_item] + toc_entries

    # Add navigation files (required by EPUB spec)
    book.add_item(epub.EpubNcx())
    book.add_item(epub.EpubNav())

    # Set spine (reading order)
    book.spine = ["nav", intro_item] + chapter_items

    # Write the EPUB file
    epub.write_epub(output_path, book, {})
    print(f"EPUB written to: {output_path}")
    print(f"  {len(chapters)} chapters, {len(analysis_names)} analyses per chapter")


# ── CLI ──────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compile processed podcasts into an EPUB book.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # By channel name (exact match)
  python compile.py --channel "MIT OpenCourseWare" \\
      --title "MIT 15.773: Hands-on Deep Learning" \\
      --author "MIT OpenCourseWare" \\
      --output "MIT-15773.epub"

  # By folder substring
  python compile.py --filter "mit-opencourseware" \\
      --title "MIT 15.773" --output "MIT-15773.epub"

  # Preview without building
  python compile.py --channel "MIT OpenCourseWare" --dry-run
        """,
    )

    # Filter options (mutually exclusive)
    filter_group = parser.add_mutually_exclusive_group(required=True)
    filter_group.add_argument(
        "--channel",
        help="Filter by exact channel name (case-insensitive)",
    )
    filter_group.add_argument(
        "--filter",
        help="Filter by substring in folder name",
    )
    filter_group.add_argument(
        "--folders",
        nargs="+",
        help="Explicit list of folder names to include",
    )

    # EPUB metadata
    parser.add_argument(
        "--title",
        default="Compiled Lectures",
        help="Book title (default: 'Compiled Lectures')",
    )
    parser.add_argument(
        "--author",
        default="Pod Transcriber",
        help="Book author (default: 'Pod Transcriber')",
    )
    parser.add_argument(
        "--output", "-o",
        default="output.epub",
        help="Output EPUB file path (default: 'output.epub')",
    )

    # Content options
    parser.add_argument(
        "--analyses",
        nargs="+",
        default=["summary", "concept-map"],
        help="Which analyses to include (default: summary concept-map)",
    )
    parser.add_argument(
        "--no-transcript",
        action="store_true",
        help="Omit transcript sections from chapters",
    )

    # Preview
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be compiled without building the EPUB",
    )

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    # Load all podcasts
    all_podcasts = list_podcasts()
    if not all_podcasts:
        print("No podcasts found in the podcasts directory.", file=sys.stderr)
        sys.exit(1)

    # Apply filter
    if args.channel:
        filtered = filter_by_channel(all_podcasts, args.channel)
        filter_desc = f'channel "{args.channel}"'
    elif args.filter:
        filtered = filter_by_pattern(all_podcasts, args.filter)
        filter_desc = f'pattern "{args.filter}"'
    else:
        filtered = filter_by_folders(all_podcasts, args.folders)
        filter_desc = f"{len(args.folders)} specified folders"

    if not filtered:
        print(f"No podcasts matched {filter_desc}.", file=sys.stderr)
        sys.exit(1)

    # Sort into lecture order
    sorted_lectures = sort_lectures(filtered)

    # Dry run — just show what we'd compile
    if args.dry_run:
        print(f"Found {len(sorted_lectures)} podcasts matching {filter_desc}:\n")
        for i, p in enumerate(sorted_lectures, 1):
            title = p.get("metadata", {}).get("title", p["folder"])
            dur = format_duration(
                p.get("metadata", {}).get("duration_seconds", 0)
            )
            analyses = p.get("analyses", [])
            print(f"  {i:2d}. {title}")
            print(f"      Duration: {dur}  |  Analyses: {', '.join(analyses)}")
        total = sum(
            p.get("metadata", {}).get("duration_seconds", 0)
            for p in sorted_lectures
        )
        print(f"\nTotal duration: {format_duration(total)}")
        print(f"Analyses to include: {', '.join(args.analyses)}")
        print(f"Include transcript: {not args.no_transcript}")
        return

    # Load full data for each podcast
    print(f"Loading {len(sorted_lectures)} podcasts...")
    full_data = []
    for p in sorted_lectures:
        data = get_podcast(p["folder"])
        if data is None:
            print(f"  Warning: could not load {p['folder']}, skipping")
            continue
        full_data.append(data)

    if not full_data:
        print("No podcast data could be loaded.", file=sys.stderr)
        sys.exit(1)

    # Build EPUB
    print(f"Building EPUB: {args.title}")
    build_epub(
        title=args.title,
        author=args.author,
        chapters=full_data,
        analysis_names=args.analyses,
        include_transcript=not args.no_transcript,
        output_path=args.output,
    )


if __name__ == "__main__":
    main()
