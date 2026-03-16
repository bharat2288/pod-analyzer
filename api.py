"""
FastAPI backend for Podcast Breakdown Pipeline web UI.

Provides endpoints for browsing and reading processed podcasts.
Serves the frontend from /static.
"""

import asyncio
import json
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from config import PODCASTS_DIR, BASE_DIR
from storage import list_podcasts, get_podcast, save_fetch_result, save_analysis
from fetch import fetch_video, FetchResult, VideoMetadata, TranscriptSegment
from process import load_preset, load_prompt, run_analysis, list_presets, list_prompts
from search import (
    search as fts_search,
    get_channels,
    get_index_stats,
    index_podcast,
    build_full_index,
    SEARCH_DB
)

# Static files directory
STATIC_DIR = BASE_DIR / "static"

import os as _os
_debug = _os.getenv("DEBUG", "").lower() in ("1", "true", "yes")
app = FastAPI(
    title="Podcast Breakdown API",
    description="Browse and read processed podcast transcripts and analyses",
    version="0.1.0",
    docs_url="/docs" if _debug else None,
    redoc_url="/redoc" if _debug else None,
    openapi_url="/openapi.json" if _debug else None,
)

# Allow CORS for local development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8889", "http://127.0.0.1:8889"],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type", "Authorization"],
)


# === Response Models ===

class PodcastSummary(BaseModel):
    """Summary info for podcast list view."""
    folder: str
    title: str
    channel: str
    duration: Optional[str]
    publish_date: Optional[str]
    word_count: Optional[int]
    video_id: Optional[str]  # For YouTube thumbnail
    analyses: list[str]


class PodcastDetail(BaseModel):
    """Full podcast data for detail view."""
    folder: str
    metadata: dict
    transcript_text: str
    transcript_segments: list[dict]
    word_count: int
    analyses: dict[str, str]


class SearchResult(BaseModel):
    """Search result within a transcript."""
    segment_index: int
    text: str
    start: float
    context_before: str
    context_after: str


class ProcessRequest(BaseModel):
    """Request to process a new YouTube video."""
    url: str
    preset: str = "default"


class ReprocessRequest(BaseModel):
    """Request to reprocess an existing podcast with selected prompts."""
    prompts: list[str] = None  # List of prompt slugs to run
    preset: str = "default"    # Fallback if prompts not provided


class LibrarySearchRequest(BaseModel):
    """Request for searching/filtering the podcast library."""
    query: Optional[str] = None
    channel: Optional[str] = None
    sort: str = "recent"  # recent, alpha, duration
    min_duration: Optional[int] = None
    max_duration: Optional[int] = None


# === API Endpoints ===

@app.get("/api")
async def api_root():
    """API info endpoint."""
    return {
        "name": "Podcast Breakdown API",
        "version": "0.1.0",
        "endpoints": {
            "podcasts": "/podcasts",
            "podcast_detail": "/podcasts/{folder}",
            "search": "/podcasts/{folder}/search?q={query}"
        }
    }


@app.get("/podcasts", response_model=list[PodcastSummary])
async def get_podcasts():
    """
    List all processed podcasts.

    Returns summaries sorted by date (newest first).
    """
    podcasts = list_podcasts()

    results = []
    for p in podcasts:
        meta = p.get("metadata", {})

        # Get word count from transcript if available
        word_count = None
        podcast_dir = Path(p["path"])
        transcript_json = podcast_dir / "transcript.json"
        if transcript_json.exists():
            import json
            with open(transcript_json) as f:
                data = json.load(f)
                word_count = data.get("word_count")

        results.append(PodcastSummary(
            folder=p["folder"],
            title=meta.get("title", "Unknown"),
            channel=meta.get("channel", "Unknown"),
            duration=meta.get("duration_formatted"),
            publish_date=meta.get("publish_date"),
            word_count=word_count,
            video_id=meta.get("video_id"),
            analyses=p.get("analyses", [])
        ))

    return results


@app.get("/podcasts/{folder}", response_model=PodcastDetail)
async def get_podcast_detail(folder: str):
    """
    Get full details for a specific podcast.

    Includes metadata, full transcript, and all analyses.
    """
    podcast = get_podcast(folder)

    if not podcast:
        raise HTTPException(status_code=404, detail=f"Podcast not found: {folder}")

    transcript = podcast.get("transcript", {})

    return PodcastDetail(
        folder=podcast["folder"],
        metadata=podcast.get("metadata", {}),
        transcript_text=transcript.get("full_text", ""),
        transcript_segments=transcript.get("segments", []),
        word_count=transcript.get("word_count", 0),
        analyses=podcast.get("analyses", {})
    )


@app.get("/podcasts/{folder}/search")
async def search_transcript(folder: str, q: str, limit: int = 50):
    """
    Search within a podcast's transcript.

    Returns matching segments with surrounding context.
    """
    if not q or len(q) < 2:
        raise HTTPException(status_code=400, detail="Query must be at least 2 characters")

    podcast = get_podcast(folder)
    if not podcast:
        raise HTTPException(status_code=404, detail=f"Podcast not found: {folder}")

    transcript = podcast.get("transcript", {})
    segments = transcript.get("segments", [])

    # Split query into words for multi-word search (all words must match)
    query_words = q.lower().split()
    results = []

    for i, seg in enumerate(segments):
        seg_lower = seg["text"].lower()
        # Check if all query words are present in this segment
        if all(word in seg_lower for word in query_words):
            # Get extended context (3 segments before/after for better preview)
            context_before_parts = []
            for j in range(max(0, i-3), i):
                context_before_parts.append(segments[j]["text"])
            context_before = " ".join(context_before_parts)

            context_after_parts = []
            for j in range(i+1, min(len(segments), i+4)):
                context_after_parts.append(segments[j]["text"])
            context_after = " ".join(context_after_parts)

            results.append({
                "segment_index": i,
                "text": seg["text"],
                "start": seg["start"],
                "context_before": context_before,
                "context_after": context_after
            })

            if len(results) >= limit:
                break

    return {
        "query": q,
        "count": len(results),
        "results": results
    }


@app.get("/podcasts/{folder}/analyses/{analysis_name}")
async def get_analysis(folder: str, analysis_name: str):
    """
    Get a specific analysis for a podcast.
    """
    podcast = get_podcast(folder)
    if not podcast:
        raise HTTPException(status_code=404, detail=f"Podcast not found: {folder}")

    analyses = podcast.get("analyses", {})

    if analysis_name not in analyses:
        raise HTTPException(
            status_code=404,
            detail=f"Analysis not found: {analysis_name}. Available: {list(analyses.keys())}"
        )

    return {
        "name": analysis_name,
        "content": analyses[analysis_name]
    }


# === Library Search Endpoints ===

@app.get("/api/search")
async def search_library(
    q: str,
    channel: Optional[str] = None,
    min_duration: Optional[int] = None,
    max_duration: Optional[int] = None,
    limit: int = 50
):
    """
    Full-text search across all podcast content.

    Searches titles, channels, transcripts, and analysis outputs.
    Returns results with match snippets, ranked by relevance.

    Query supports FTS5 syntax:
    - Simple words: "scaling"
    - Phrases: '"embodied cognition"'
    - AND/OR: "scaling OR training"
    - Prefix: "scale*"
    """
    if not q or len(q) < 2:
        raise HTTPException(status_code=400, detail="Query must be at least 2 characters")

    try:
        results = fts_search(
            query=q,
            limit=limit,
            channel=channel,
            min_duration=min_duration,
            max_duration=max_duration
        )

        return {
            "query": q,
            "count": len(results),
            "results": results
        }

    except Exception as e:
        # FTS5 can throw on invalid syntax, return helpful error
        raise HTTPException(status_code=400, detail=f"Search error: {str(e)}")


@app.get("/api/channels")
async def list_channels():
    """
    Get all channels with podcast counts.

    Useful for building channel filter chips in the UI.
    """
    channels = get_channels()
    return {"channels": channels}


@app.get("/api/stats")
async def library_stats():
    """
    Get library statistics.

    Returns total podcast count, channel breakdown, duration ranges.
    """
    stats = get_index_stats()
    return stats


@app.get("/api/library")
async def get_library(
    channel: Optional[str] = None,
    sort: str = "recent",
    min_duration: Optional[int] = None,
    max_duration: Optional[int] = None
):
    """
    Get filtered and sorted podcast library.

    Unlike /podcasts (which returns raw list), this applies filters
    and sorting for the library UI.

    Sort options: recent, alpha, duration
    """
    podcasts = list_podcasts()

    # Apply filters
    if channel:
        podcasts = [p for p in podcasts if p.get("metadata", {}).get("channel") == channel]

    if min_duration is not None:
        podcasts = [
            p for p in podcasts
            if (p.get("metadata", {}).get("duration_seconds") or 0) >= min_duration
        ]

    if max_duration is not None:
        podcasts = [
            p for p in podcasts
            if (p.get("metadata", {}).get("duration_seconds") or 0) <= max_duration
        ]

    # Apply sorting
    if sort == "alpha":
        podcasts.sort(key=lambda p: p.get("metadata", {}).get("title", "").lower())
    elif sort == "duration":
        podcasts.sort(
            key=lambda p: p.get("metadata", {}).get("duration_seconds") or 0,
            reverse=True
        )
    # "recent" is already the default sort from list_podcasts()

    # Format response
    results = []
    for p in podcasts:
        meta = p.get("metadata", {})
        results.append({
            "folder": p["folder"],
            "title": meta.get("title", "Unknown"),
            "channel": meta.get("channel", "Unknown"),
            "duration_seconds": meta.get("duration_seconds"),
            "duration_formatted": meta.get("duration_formatted"),
            "publish_date": meta.get("publish_date"),
            "analyses": p.get("analyses", [])
        })

    return {"count": len(results), "podcasts": results}


@app.post("/api/reindex")
async def reindex_library():
    """
    Rebuild the full search index from disk.

    Use this if the index gets out of sync with files on disk.
    """
    result = build_full_index()
    return {
        "status": "success",
        "indexed": result["indexed"],
        "errors": result["errors"]
    }


# === Process Endpoint ===

@app.get("/presets")
async def get_presets():
    """List available presets with slug (filename) for API calls."""
    from config import PRESETS_DIR
    import json

    results = []
    for preset_file in sorted(PRESETS_DIR.glob("*.json")):
        try:
            with open(preset_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            results.append({
                "slug": preset_file.stem,  # filename without extension, used for API calls
                "name": data.get("name", preset_file.stem),
                "description": data.get("description", ""),
                "prompts": data.get("prompts", [])
            })
        except Exception:
            pass
    return results


@app.get("/prompts")
async def get_prompts():
    """List all available prompts with their metadata."""
    prompts = list_prompts()
    return [
        {
            "slug": p.filename.replace(".txt", ""),
            "name": p.name,
            "description": p.description
        }
        for p in prompts
    ]


@app.post("/process")
async def process_video(request: ProcessRequest):
    """
    Process a new YouTube video.

    Fetches transcript, runs preset analyses, saves results.
    Returns the folder name for the processed podcast.
    """
    try:
        # Fetch transcript and metadata
        fetch_result = fetch_video(request.url)

        # Save transcript and metadata
        podcast_dir = save_fetch_result(fetch_result)

        # Load preset and run analyses
        preset = load_preset(request.preset)
        analyses_completed = []

        for prompt_name in preset.prompts:
            prompt_config = load_prompt(prompt_name)
            result = run_analysis(prompt_config, fetch_result)

            # Save analysis
            save_analysis(
                content=result.content,
                analysis_name=prompt_name,
                podcast_dir=podcast_dir,
                prompt_file=prompt_config.filename,
                model=result.model
            )
            analyses_completed.append(prompt_name)

        # Index the new podcast for search
        index_podcast(podcast_dir)

        return {
            "status": "success",
            "folder": podcast_dir.name,
            "title": fetch_result.metadata.title,
            "analyses_completed": analyses_completed
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/process/stream")
async def process_video_stream(url: str, prompts: str = None, preset: str = "default"):
    """
    Process a new YouTube video with Server-Sent Events for progress updates.

    Args:
        url: YouTube video URL
        prompts: Comma-separated list of prompt slugs to run (e.g., "summary,key_claims")
        preset: Preset name (only used if prompts is not provided)

    Streams progress events as the video is fetched and analyzed.
    Each event is a JSON object with stage, status, and message fields.
    """
    # Determine which prompts to run
    if prompts:
        prompt_list = [p.strip() for p in prompts.split(",") if p.strip()]
    else:
        preset_config = load_preset(preset)
        prompt_list = preset_config.prompts

    async def event_generator():
        try:
            # Helper to send SSE events
            def send_event(data: dict):
                return f"data: {json.dumps(data)}\n\n"

            # Stage 1: Fetching transcript
            yield send_event({
                "stage": "fetch",
                "status": "running",
                "message": "Fetching transcript from YouTube..."
            })

            # Run fetch in thread pool to not block
            loop = asyncio.get_event_loop()
            fetch_result = await loop.run_in_executor(None, fetch_video, url)

            yield send_event({
                "stage": "fetch",
                "status": "done",
                "message": f"Fetched: {fetch_result.metadata.title}",
                "title": fetch_result.metadata.title,
                "duration": fetch_result.metadata.duration_formatted,
                "word_count": fetch_result.word_count
            })

            # Stage 2: Saving transcript
            yield send_event({
                "stage": "save",
                "status": "running",
                "message": "Saving transcript..."
            })

            podcast_dir = await loop.run_in_executor(None, save_fetch_result, fetch_result)

            yield send_event({
                "stage": "save",
                "status": "done",
                "message": "Transcript saved",
                "folder": podcast_dir.name
            })

            # Stage 3: Running analyses
            total_prompts = len(prompt_list)
            analyses_completed = []

            for i, prompt_name in enumerate(prompt_list):
                yield send_event({
                    "stage": "analysis",
                    "status": "running",
                    "prompt": prompt_name,
                    "current": i + 1,
                    "total": total_prompts,
                    "message": f"Running {prompt_name.replace('_', ' ')}... ({i + 1}/{total_prompts})"
                })

                prompt_config = load_prompt(prompt_name)

                # Run LLM analysis in thread pool
                result = await loop.run_in_executor(
                    None,
                    run_analysis,
                    prompt_config,
                    fetch_result
                )

                # Save analysis
                await loop.run_in_executor(
                    None,
                    lambda: save_analysis(
                        content=result.content,
                        analysis_name=prompt_name,
                        podcast_dir=podcast_dir,
                        prompt_file=prompt_config.filename,
                        model=result.model
                    )
                )

                analyses_completed.append(prompt_name)

                yield send_event({
                    "stage": "analysis",
                    "status": "done",
                    "prompt": prompt_name,
                    "current": i + 1,
                    "total": total_prompts,
                    "message": f"Completed {prompt_name.replace('_', ' ')}"
                })

            # Stage 4: Indexing for search
            yield send_event({
                "stage": "index",
                "status": "running",
                "message": "Indexing for search..."
            })

            await loop.run_in_executor(None, index_podcast, podcast_dir)

            yield send_event({
                "stage": "index",
                "status": "done",
                "message": "Indexed for search"
            })

            # Final success event
            yield send_event({
                "stage": "complete",
                "status": "success",
                "folder": podcast_dir.name,
                "title": fetch_result.metadata.title,
                "analyses_completed": analyses_completed,
                "message": "Processing complete!"
            })

        except Exception as e:
            yield send_event({
                "stage": "error",
                "status": "error",
                "message": str(e)
            })

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"  # Disable nginx buffering
        }
    )


@app.post("/podcasts/{folder}/reprocess")
async def reprocess_podcast(folder: str, request: ReprocessRequest):
    """
    Reprocess an existing podcast with selected prompts.

    Runs the specified analyses on the existing transcript.
    """
    # Get existing podcast data
    podcast = get_podcast(folder)
    if not podcast:
        raise HTTPException(status_code=404, detail=f"Podcast not found: {folder}")

    try:
        # Determine which prompts to run
        if request.prompts:
            prompt_list = request.prompts
        else:
            preset = load_preset(request.preset)
            prompt_list = preset.prompts

        analyses_completed = []

        # Get transcript and metadata from stored podcast
        transcript_data = podcast.get("transcript", {})
        metadata_dict = podcast.get("metadata", {})

        # Reconstruct proper FetchResult from stored data
        # Create VideoMetadata from stored metadata
        video_metadata = VideoMetadata(
            video_id=metadata_dict.get("video_id", ""),
            url=metadata_dict.get("url", ""),
            title=metadata_dict.get("title", ""),
            channel=metadata_dict.get("channel", ""),
            channel_id=metadata_dict.get("channel_id"),
            duration_seconds=metadata_dict.get("duration_seconds"),
            duration_formatted=metadata_dict.get("duration_formatted"),
            publish_date=metadata_dict.get("publish_date"),
            fetched_at=metadata_dict.get("fetched_at")
        )

        # Create TranscriptSegment objects from stored segments
        segments = [
            TranscriptSegment(
                text=seg.get("text", ""),
                start=seg.get("start", 0.0),
                duration=seg.get("duration", 0.0)
            )
            for seg in transcript_data.get("segments", [])
        ]

        # Create FetchResult
        fetch_result = FetchResult(
            metadata=video_metadata,
            segments=segments,
            full_text=transcript_data.get("full_text", ""),
            word_count=transcript_data.get("word_count", 0)
        )

        # Get podcast directory
        podcast_dir = PODCASTS_DIR / folder

        for prompt_name in prompt_list:
            prompt_config = load_prompt(prompt_name)
            result = run_analysis(prompt_config, fetch_result)

            # Save analysis
            save_analysis(
                content=result.content,
                analysis_name=prompt_name,
                podcast_dir=podcast_dir,
                prompt_file=prompt_config.filename,
                model=result.model
            )
            analyses_completed.append(prompt_name)

        # Re-index to include new analyses
        index_podcast(podcast_dir)

        return {
            "status": "success",
            "folder": folder,
            "analyses_completed": analyses_completed
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# === Frontend ===

@app.get("/")
async def serve_frontend():
    """Serve the frontend HTML."""
    index_path = STATIC_DIR / "index.html"
    if index_path.exists():
        return FileResponse(index_path)
    return {"message": "Frontend not found. Place index.html in static/"}


# Mount static files (for any additional assets like CSS, JS)
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# === Run with: python api.py ===
if __name__ == "__main__":
    import uvicorn
    print("\n  Podcast Breakdown UI")
    print("  " + "-" * 28)
    print("  Open http://127.0.0.1:8889 in your browser")
    print()
    uvicorn.run(app, host="127.0.0.1", port=8889)
