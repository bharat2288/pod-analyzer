---
type: project-home
project: pod-transcriber
date: 2026-03-07
cssclasses:
  - project-home
---
# Pod Transcriber
*[[dev-hub|Hub]] · [[README|GitHub]]*

YouTube podcast transcript processing pipeline. Configurable LLM analysis via modular prompts, local web UI for browsing processed content.

## Specs

```dataview
TABLE rows.file.link as Specs
FROM "pod-transcriber/specs"
WHERE type AND type != "spec-prompts"
GROUP BY type
SORT type ASC
```
> [!warning]- Open Errors (`$= dv.pages('"knowledge/exports/errors"').where(p => p.project == "pod-transcriber" && !p.resolved).length`)
> ```dataview
> TABLE module, date
> FROM "knowledge/exports/errors"
> WHERE project = "pod-transcriber" AND resolved = false
> SORT date DESC
> LIMIT 5
> ```

> [!info]- Decisions (`$= dv.pages('"knowledge/exports/decisions"').where(p => p.project == "pod-transcriber").length`)
> ```dataview
> TABLE date
> FROM "knowledge/exports/decisions"
> WHERE project = "pod-transcriber"
> SORT date DESC
> LIMIT 5
> ```
>
> > [!info]- All Decisions
> > ```dataview
> > TABLE date
> > FROM "knowledge/exports/decisions"
> > WHERE project = "pod-transcriber"
> > SORT date DESC
> > ```

> [!tip]- Learnings (`$= dv.pages('"knowledge/exports/learnings"').where(p => p.project == "pod-transcriber").length`)
> ```dataview
> TABLE tags
> FROM "knowledge/exports/learnings"
> WHERE project = "pod-transcriber"
> SORT date DESC
> LIMIT 5
> ```
>
> > [!tip]- All Learnings
> > ```dataview
> > TABLE tags
> > FROM "knowledge/exports/learnings"
> > WHERE project = "pod-transcriber"
> > SORT date DESC
> > ```

> [!abstract]- Project Plans (`$= dv.pages('"knowledge/plans"').where(p => p.project == "pod-transcriber").length`)
> ```dataview
> TABLE title, default(date, file.ctime) as Date
> FROM "knowledge/plans"
> WHERE project = "pod-transcriber"
> SORT default(date, file.ctime) DESC
> ```

> [!note]- Sessions (`$= dv.pages('"knowledge/sessions/pod-transcriber"').length`)
> ```dataview
> TABLE topic
> FROM "knowledge/sessions/pod-transcriber"
> SORT file.mtime DESC
> LIMIT 5
> ```
>
> > [!note]- All Sessions
> > ```dataview
> > TABLE topic
> > FROM "knowledge/sessions/pod-transcriber"
> > SORT file.mtime DESC
> > ```
