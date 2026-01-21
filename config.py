"""
Configuration for Podcast Breakdown Pipeline.

Paths, defaults, and settings. API keys loaded from environment variables.
"""

import os
from pathlib import Path

# Load .env file if python-dotenv is available
try:
    from dotenv import load_dotenv
    # Load project-local .env file
    local_env = Path(__file__).parent / ".env"
    if local_env.exists():
        load_dotenv(local_env, override=True)
except ImportError:
    pass  # python-dotenv not installed, rely on system env vars

# === Paths ===
# Base directory is where this config file lives
BASE_DIR = Path(__file__).parent

# Output folder for processed podcasts
PODCASTS_DIR = BASE_DIR / "podcasts"

# Prompt templates folder
PROMPTS_DIR = BASE_DIR / "prompts"

# Preset configurations folder
PRESETS_DIR = BASE_DIR / "presets"

# === API Configuration ===
# Load API keys from environment variables
# Set these in your shell or .env file:
#   export OPENAI_API_KEY="sk-..."
#   export ANTHROPIC_API_KEY="sk-ant-..."

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

# Default LLM to use for analysis
# Options: "openai", "anthropic"
DEFAULT_LLM_PROVIDER = "anthropic"

# Model names per provider
LLM_MODELS = {
    "openai": "gpt-4o",
    "anthropic": "claude-opus-4-5-20251101",
}

# === Defaults ===
# Default preset to run when no --preset flag provided
DEFAULT_PRESET = "default"

# Whether to show interactive menu after default analyses
DEFAULT_INTERACTIVE = True

# === Folder Naming ===
# Format for podcast folders: {date}_{channel}_{title}
# Example: 2025-01-10_lex-fridman_sam-altman-openai
FOLDER_DATE_FORMAT = "%Y-%m-%d"
MAX_SLUG_LENGTH = 50  # Max length for channel/title slugs


def get_api_key(provider: str = None) -> str:
    """
    Get the API key for the specified provider.

    Args:
        provider: "openai" or "anthropic". If None, uses DEFAULT_LLM_PROVIDER.

    Returns:
        The API key string.

    Raises:
        ValueError: If no API key is configured for the provider.
    """
    provider = provider or DEFAULT_LLM_PROVIDER

    if provider == "openai":
        if not OPENAI_API_KEY:
            raise ValueError(
                "OpenAI API key not found. "
                "Set OPENAI_API_KEY environment variable."
            )
        return OPENAI_API_KEY

    elif provider == "anthropic":
        if not ANTHROPIC_API_KEY:
            raise ValueError(
                "Anthropic API key not found. "
                "Set ANTHROPIC_API_KEY environment variable."
            )
        return ANTHROPIC_API_KEY

    else:
        raise ValueError(f"Unknown provider: {provider}")


def ensure_directories():
    """Create required directories if they don't exist."""
    PODCASTS_DIR.mkdir(exist_ok=True)
    PROMPTS_DIR.mkdir(exist_ok=True)
    (PROMPTS_DIR / "custom").mkdir(exist_ok=True)
    PRESETS_DIR.mkdir(exist_ok=True)
