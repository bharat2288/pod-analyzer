# Pod Analyzer

CLI tool that fetches YouTube transcripts and runs configurable LLM analyses (summaries, key claims, concept maps) using Claude, GPT, or OpenRouter.

## What It Does

1. **Fetch** — Downloads transcript and metadata from any YouTube video (no API key required)
2. **Process** — Runs customizable prompt-based analyses using your choice of LLM
3. **Store** — Organizes outputs in a clean folder structure with timestamped transcripts
4. **Search** — Full-text search across all your processed podcasts

## Features

- Multiple LLM providers: Anthropic (Claude), OpenAI (GPT), OpenRouter
- Configurable analysis presets (quick, deep dive, academic, social)
- Custom prompt templates with frontmatter configuration
- Timestamped transcript storage (JSON + plain text)
- Interactive menu for running additional analyses
- Full-text search with SQLite FTS5

## Installation

```bash
# Clone the repo
git clone https://github.com/yourusername/pod-analyzer.git
cd pod-analyzer

# Create virtual environment (recommended)
python -m venv venv
venv\Scripts\activate  # Windows
# source venv/bin/activate  # macOS/Linux

# Install dependencies
pip install -r requirements.txt
```

## Configuration

Create a `.env` file in the project root:

```env
# Required: At least one API key
ANTHROPIC_API_KEY=sk-ant-...
OPENAI_API_KEY=sk-...
OPENROUTER_API_KEY=sk-or-...

# Optional: Override default models
# ANTHROPIC_MODEL=claude-sonnet-4-20250514
# OPENAI_MODEL=gpt-4o
# OPENROUTER_MODEL=google/gemini-2.5-pro
```

## Usage

### Basic Usage

```bash
# Process a YouTube video with default preset
python podcast_breakdown.py "https://www.youtube.com/watch?v=VIDEO_ID"

# Use a specific preset
python podcast_breakdown.py "https://youtu.be/VIDEO_ID" --preset deep_dive

# Skip interactive menu
python podcast_breakdown.py "https://youtu.be/VIDEO_ID" --no-interactive

# Use a specific LLM provider
python podcast_breakdown.py "https://youtu.be/VIDEO_ID" --provider openai
```

### Search Your Podcasts

```bash
# Build/update search index
python search.py --index

# Search for a term
python search.py "artificial intelligence"

# Search with more context
python search.py "transformer architecture" --context 200
```

### Available Presets

| Preset | Analyses | Best For |
|--------|----------|----------|
| `default` | summary, key_claims | Quick overview |
| `quick` | summary only | Fast processing |
| `deep_dive` | summary, key_claims, concept_map, theorize, counterarguments | Thorough analysis |
| `academic` | summary, key_claims, theorize, quotables | Research purposes |
| `social` | summary, quotables | Social sharing |

### Custom Prompts

Add custom prompts to `prompts/custom/`:

```txt
---
name: My Custom Analysis
description: Does something specific
model: default
max_tokens: 2000
---

Analyze the following transcript for {topic}:

{transcript}

Provide your analysis in markdown format.
```

Available template variables:
- `{transcript}` — Timestamped transcript
- `{transcript_plain}` — Plain text without timestamps
- `{title}` — Video title
- `{channel}` — Channel name
- `{metadata}` — Full metadata as JSON

## Output Structure

```
podcasts/
└── 2025-01-15_lex-fridman_sam-altman-openai/
    ├── metadata.json      # Video metadata
    ├── transcript.json    # Timestamped segments
    ├── transcript.txt     # Plain text transcript
    └── analyses/
        ├── summary.md
        ├── key-claims.md
        └── concept-map.md
```

## Tech Stack

- **Transcripts**: youtube-transcript-api, yt-dlp (for metadata)
- **LLM APIs**: anthropic, openai
- **Search**: SQLite FTS5
- **Config**: python-dotenv

## Requirements

- Python 3.10+
- API key for at least one LLM provider

## License

MIT

---

Built with Claude Code
