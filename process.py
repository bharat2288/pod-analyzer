"""
Process module for Podcast Breakdown Pipeline.

Handles loading prompts, running LLM analyses, and orchestrating the pipeline.
"""

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import anthropic
import openai

from config import (
    DEFAULT_LLM_PROVIDER,
    DEFAULT_PRESET,
    LLM_MODELS,
    PRESETS_DIR,
    PROMPTS_DIR,
    get_api_key,
)
from fetch import FetchResult


@dataclass
class PromptConfig:
    """Configuration parsed from a prompt template file."""
    name: str
    description: str
    model: str  # "default" uses config setting, or specific model name
    max_tokens: int
    template: str  # The actual prompt text with {placeholders}
    filename: str  # Original filename for reference


@dataclass
class PresetConfig:
    """Configuration for a preset (set of prompts to run)."""
    name: str
    description: str
    prompts: list[str]  # List of prompt names to run


@dataclass
class AnalysisResult:
    """Result from running a single analysis."""
    prompt_name: str
    content: str
    model: str
    tokens_used: Optional[int] = None


def parse_frontmatter(content: str) -> tuple[dict, str]:
    """
    Parse YAML-style frontmatter from a prompt template.

    Args:
        content: Full file content

    Returns:
        Tuple of (frontmatter dict, remaining content)
    """
    # Check if content starts with frontmatter delimiter
    if not content.startswith("---"):
        return {}, content

    # Find the closing delimiter
    lines = content.split("\n")
    end_index = None
    for i, line in enumerate(lines[1:], start=1):
        if line.strip() == "---":
            end_index = i
            break

    if end_index is None:
        return {}, content

    # Parse frontmatter (simple key: value parsing)
    frontmatter = {}
    for line in lines[1:end_index]:
        if ":" in line:
            key, value = line.split(":", 1)
            key = key.strip()
            value = value.strip()
            # Try to parse as int
            try:
                value = int(value)
            except ValueError:
                pass
            frontmatter[key] = value

    # Get the template content (everything after frontmatter)
    template = "\n".join(lines[end_index + 1:]).strip()

    return frontmatter, template


def load_prompt(prompt_name: str, prompts_dir: Optional[Path] = None) -> PromptConfig:
    """
    Load a prompt template from the prompts folder.

    Args:
        prompt_name: Name of the prompt (without .txt extension)
        prompts_dir: Directory to search (default: PROMPTS_DIR)

    Returns:
        PromptConfig with parsed settings and template

    Raises:
        FileNotFoundError: If prompt file doesn't exist
    """
    prompts_dir = prompts_dir or PROMPTS_DIR

    # Look for prompt file
    prompt_file = prompts_dir / f"{prompt_name}.txt"
    if not prompt_file.exists():
        # Check custom folder
        prompt_file = prompts_dir / "custom" / f"{prompt_name}.txt"
        if not prompt_file.exists():
            raise FileNotFoundError(f"Prompt not found: {prompt_name}")

    # Read and parse
    content = prompt_file.read_text(encoding="utf-8")
    frontmatter, template = parse_frontmatter(content)

    return PromptConfig(
        name=frontmatter.get("name", prompt_name.replace("_", " ").title()),
        description=frontmatter.get("description", ""),
        model=frontmatter.get("model", "default"),
        max_tokens=frontmatter.get("max_tokens", 2000),
        template=template,
        filename=prompt_file.name
    )


def load_preset(preset_name: str, presets_dir: Optional[Path] = None) -> PresetConfig:
    """
    Load a preset configuration.

    Args:
        preset_name: Name of the preset (without .json extension)
        presets_dir: Directory to search (default: PRESETS_DIR)

    Returns:
        PresetConfig with list of prompts to run

    Raises:
        FileNotFoundError: If preset file doesn't exist
    """
    presets_dir = presets_dir or PRESETS_DIR

    preset_file = presets_dir / f"{preset_name}.json"
    if not preset_file.exists():
        raise FileNotFoundError(f"Preset not found: {preset_name}")

    with open(preset_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    return PresetConfig(
        name=data.get("name", preset_name),
        description=data.get("description", ""),
        prompts=data.get("prompts", [])
    )


def list_prompts(prompts_dir: Optional[Path] = None) -> list[PromptConfig]:
    """
    List all available prompts.

    Args:
        prompts_dir: Directory to search (default: PROMPTS_DIR)

    Returns:
        List of PromptConfig objects
    """
    prompts_dir = prompts_dir or PROMPTS_DIR
    prompts = []

    # Search main prompts folder
    for prompt_file in prompts_dir.glob("*.txt"):
        try:
            prompts.append(load_prompt(prompt_file.stem, prompts_dir))
        except Exception:
            pass

    # Search custom folder
    custom_dir = prompts_dir / "custom"
    if custom_dir.exists():
        for prompt_file in custom_dir.glob("*.txt"):
            try:
                prompts.append(load_prompt(prompt_file.stem, prompts_dir))
            except Exception:
                pass

    return prompts


def list_presets(presets_dir: Optional[Path] = None) -> list[PresetConfig]:
    """
    List all available presets.

    Args:
        presets_dir: Directory to search (default: PRESETS_DIR)

    Returns:
        List of PresetConfig objects
    """
    presets_dir = presets_dir or PRESETS_DIR
    presets = []

    for preset_file in presets_dir.glob("*.json"):
        try:
            presets.append(load_preset(preset_file.stem, presets_dir))
        except Exception:
            pass

    return presets


def format_timestamp(seconds: float) -> str:
    """Convert seconds to HH:MM:SS or MM:SS format."""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    if hours > 0:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes}:{secs:02d}"


def build_timestamped_transcript(fetch_result: FetchResult) -> str:
    """
    Build a transcript with timestamps for each segment.

    Format:
    [0:00] First segment text
    [0:15] Second segment text
    """
    lines = []
    for segment in fetch_result.segments:
        ts = format_timestamp(segment.start)
        lines.append(f"[{ts}] {segment.text}")
    return "\n".join(lines)


def fill_template(template: str, fetch_result: FetchResult) -> str:
    """
    Fill in template variables with actual data.

    Supported variables:
        {transcript} - Timestamped transcript
        {transcript_plain} - Plain text without timestamps
        {metadata} - JSON metadata blob
        {title} - Video title
        {channel} - Channel name

    Args:
        template: Prompt template with {placeholders}
        fetch_result: FetchResult with data to fill in

    Returns:
        Filled template string
    """
    # Build metadata dict
    metadata = {
        "video_id": fetch_result.metadata.video_id,
        "title": fetch_result.metadata.title,
        "channel": fetch_result.metadata.channel,
        "duration": fetch_result.metadata.duration_formatted,
        "publish_date": fetch_result.metadata.publish_date,
        "word_count": fetch_result.word_count,
    }

    # Build timestamped transcript
    timestamped = build_timestamped_transcript(fetch_result)

    # Fill in placeholders
    filled = template
    filled = filled.replace("{transcript}", timestamped)
    filled = filled.replace("{transcript_plain}", fetch_result.full_text)
    filled = filled.replace("{metadata}", json.dumps(metadata, indent=2))
    filled = filled.replace("{title}", fetch_result.metadata.title)
    filled = filled.replace("{channel}", fetch_result.metadata.channel)

    return filled


def run_analysis_anthropic(
    prompt: str,
    model: str,
    max_tokens: int,
    api_key: str
) -> tuple[str, int]:
    """
    Run analysis using Anthropic's Claude API.

    Args:
        prompt: The filled prompt to send
        model: Model name (e.g., "claude-sonnet-4-20250514")
        max_tokens: Maximum tokens for response
        api_key: Anthropic API key

    Returns:
        Tuple of (response text, tokens used)
    """
    client = anthropic.Anthropic(api_key=api_key)

    message = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        messages=[
            {"role": "user", "content": prompt}
        ]
    )

    # Extract text from response
    response_text = message.content[0].text
    tokens_used = message.usage.input_tokens + message.usage.output_tokens

    return response_text, tokens_used


def run_analysis_openai(
    prompt: str,
    model: str,
    max_tokens: int,
    api_key: str
) -> tuple[str, int]:
    """
    Run analysis using OpenAI's API.

    Args:
        prompt: The filled prompt to send
        model: Model name (e.g., "gpt-4o")
        max_tokens: Maximum tokens for response
        api_key: OpenAI API key

    Returns:
        Tuple of (response text, tokens used)
    """
    client = openai.OpenAI(api_key=api_key)

    response = client.chat.completions.create(
        model=model,
        max_tokens=max_tokens,
        messages=[
            {"role": "user", "content": prompt}
        ]
    )

    response_text = response.choices[0].message.content
    tokens_used = response.usage.total_tokens if response.usage else 0

    return response_text, tokens_used


def run_analysis(
    prompt_config: PromptConfig,
    fetch_result: FetchResult,
    provider: Optional[str] = None
) -> AnalysisResult:
    """
    Run a single analysis using the specified prompt.

    Args:
        prompt_config: Loaded prompt configuration
        fetch_result: FetchResult with transcript data
        provider: LLM provider ("anthropic" or "openai"). If None, uses default.

    Returns:
        AnalysisResult with generated content
    """
    provider = provider or DEFAULT_LLM_PROVIDER

    # Fill in the template
    filled_prompt = fill_template(prompt_config.template, fetch_result)

    # Determine model
    if prompt_config.model == "default":
        model = LLM_MODELS[provider]
    else:
        model = prompt_config.model

    # Get API key
    api_key = get_api_key(provider)

    # Run the analysis
    if provider == "anthropic":
        response_text, tokens_used = run_analysis_anthropic(
            filled_prompt, model, prompt_config.max_tokens, api_key
        )
    elif provider == "openai":
        response_text, tokens_used = run_analysis_openai(
            filled_prompt, model, prompt_config.max_tokens, api_key
        )
    else:
        raise ValueError(f"Unknown provider: {provider}")

    return AnalysisResult(
        prompt_name=prompt_config.name,
        content=response_text,
        model=model,
        tokens_used=tokens_used
    )


def run_preset(
    fetch_result: FetchResult,
    preset_name: Optional[str] = None,
    provider: Optional[str] = None,
    on_progress: Optional[callable] = None
) -> list[AnalysisResult]:
    """
    Run all analyses defined in a preset.

    Args:
        fetch_result: FetchResult with transcript data
        preset_name: Name of preset to run (default: DEFAULT_PRESET)
        provider: LLM provider to use
        on_progress: Optional callback(prompt_name, status) for progress updates

    Returns:
        List of AnalysisResult objects
    """
    preset_name = preset_name or DEFAULT_PRESET
    preset = load_preset(preset_name)

    results = []
    for prompt_name in preset.prompts:
        if on_progress:
            on_progress(prompt_name, "running")

        try:
            prompt_config = load_prompt(prompt_name)
            result = run_analysis(prompt_config, fetch_result, provider)
            results.append(result)

            if on_progress:
                on_progress(prompt_name, "done", result.tokens_used)

        except Exception as e:
            if on_progress:
                on_progress(prompt_name, "error", str(e))
            raise

    return results


# === CLI for testing ===
if __name__ == "__main__":
    print("Available prompts:")
    for prompt in list_prompts():
        print(f"  - {prompt.name}: {prompt.description}")

    print("\nAvailable presets:")
    for preset in list_presets():
        print(f"  - {preset.name}: {preset.description}")
        print(f"    Prompts: {', '.join(preset.prompts)}")
