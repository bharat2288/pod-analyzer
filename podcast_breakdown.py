#!/usr/bin/env python3
"""
Podcast Breakdown Pipeline — Main CLI

Process YouTube podcast transcripts with configurable LLM analyses.

Usage:
    python podcast_breakdown.py <youtube_url>
    python podcast_breakdown.py <youtube_url> --preset deep_dive
    python podcast_breakdown.py <youtube_url> --no-interactive
"""

import argparse
import sys
from pathlib import Path

from config import DEFAULT_PRESET, DEFAULT_INTERACTIVE, ensure_directories
from fetch import fetch_video
from process import (
    list_prompts,
    load_prompt,
    run_analysis,
    run_preset,
)
from storage import save_fetch_result, save_analysis


def print_header():
    """Print the CLI header."""
    print()
    print("  Podcast Breakdown Pipeline")
    print("  " + "─" * 32)
    print()


def print_progress(prompt_name: str, status: str, extra=None):
    """Print progress updates during analysis."""
    if status == "running":
        print(f"  → Running: {prompt_name}...", end="", flush=True)
    elif status == "done":
        tokens = f" ({extra:,} tokens)" if extra else ""
        print(f" ✓{tokens}")
    elif status == "error":
        print(f" ✗ Error: {extra}")


def interactive_menu(
    fetch_result,
    podcast_dir: Path,
    completed_prompts: list[str],
    provider: str = None
):
    """
    Show interactive menu for additional analyses.

    Args:
        fetch_result: FetchResult with transcript data
        podcast_dir: Path to podcast folder for saving
        completed_prompts: List of already-run prompt names
        provider: LLM provider to use
    """
    # Get available prompts (excluding already run)
    all_prompts = list_prompts()
    available = [p for p in all_prompts if p.filename.replace(".txt", "") not in completed_prompts]

    if not available:
        print("\n  All available analyses have been run.")
        return

    while True:
        print()
        print("  " + "─" * 32)
        print("  Run additional analyses?")
        print()

        for i, prompt in enumerate(available, 1):
            desc = f" — {prompt.description}" if prompt.description else ""
            print(f"    [{i}] {prompt.name}{desc}")

        print()
        print("    [q] Done")
        print()

        choice = input("  Choice: ").strip().lower()

        if choice == "q" or choice == "":
            break

        try:
            idx = int(choice) - 1
            if 0 <= idx < len(available):
                selected = available[idx]
                prompt_name = selected.filename.replace(".txt", "")

                print()
                print_progress(selected.name, "running")

                try:
                    prompt_config = load_prompt(prompt_name)
                    result = run_analysis(prompt_config, fetch_result, provider)
                    print_progress(selected.name, "done", result.tokens_used)

                    # Save the analysis
                    save_analysis(
                        content=result.content,
                        analysis_name=prompt_name,
                        podcast_dir=podcast_dir,
                        prompt_file=selected.filename,
                        model=result.model
                    )

                    # Remove from available list
                    available = [p for p in available if p.filename != selected.filename]
                    completed_prompts.append(prompt_name)

                except Exception as e:
                    print_progress(selected.name, "error", str(e))

            else:
                print("  Invalid choice.")
        except ValueError:
            print("  Invalid choice.")


def main():
    """Main entry point."""
    # Fix Windows encoding
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

    parser = argparse.ArgumentParser(
        description="Process YouTube podcast transcripts with LLM analyses."
    )
    parser.add_argument(
        "url",
        help="YouTube video URL"
    )
    parser.add_argument(
        "--preset",
        default=DEFAULT_PRESET,
        help=f"Preset to run (default: {DEFAULT_PRESET})"
    )
    parser.add_argument(
        "--no-interactive",
        action="store_true",
        help="Skip interactive menu after preset"
    )
    parser.add_argument(
        "--provider",
        choices=["anthropic", "openai"],
        help="LLM provider to use"
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Output directory for podcasts"
    )

    args = parser.parse_args()

    # Ensure directories exist
    ensure_directories()

    print_header()

    # === Fetch ===
    print("  Fetching transcript...")
    try:
        fetch_result = fetch_video(args.url)
    except ValueError as e:
        print(f"  ✗ Error: {e}")
        sys.exit(1)

    # Print video info
    print(f"  ✓ \"{fetch_result.metadata.title}\"")
    print(f"    Channel: {fetch_result.metadata.channel}")
    duration = fetch_result.metadata.duration_formatted or "Unknown"
    print(f"    Duration: {duration} ({len(fetch_result.segments):,} segments)")
    print()

    # === Save transcript ===
    podcast_dir = save_fetch_result(fetch_result, args.output)
    print(f"  Saved to: {podcast_dir.name}/")
    print()

    # === Run preset analyses ===
    print(f"  Running preset: [{args.preset}]")
    completed_prompts = []

    try:
        results = run_preset(
            fetch_result,
            preset_name=args.preset,
            provider=args.provider,
            on_progress=print_progress
        )

        # Save each analysis
        for result in results:
            prompt_name = result.prompt_name.lower().replace(" ", "_")
            save_analysis(
                content=result.content,
                analysis_name=prompt_name,
                podcast_dir=podcast_dir,
                model=result.model
            )
            completed_prompts.append(prompt_name)

    except Exception as e:
        print(f"\n  ✗ Analysis failed: {e}")
        sys.exit(1)

    print()
    print(f"  ✓ Analyses saved to: {podcast_dir.name}/analyses/")

    # === Interactive menu ===
    interactive = DEFAULT_INTERACTIVE and not args.no_interactive

    if interactive:
        interactive_menu(
            fetch_result,
            podcast_dir,
            completed_prompts,
            provider=args.provider
        )

    print()
    print("  Done!")
    print()


if __name__ == "__main__":
    main()
