---
type: project-home
project: pod-transcriber
date: 2026-03-07
cssclasses:
  - project-home
---
# Pod Transcriber
*[[dev-hub|Hub]] · [[README|GitHub]]*
<span class="hub-status">&mdash;</span>

YouTube podcast transcript processing pipeline. Configurable LLM analysis via modular prompts, local web UI for browsing processed content.

## Specs

```base
filters:
  and:
    - file.folder.contains("pod-transcriber/specs")
    - type != "spec-prompts"
properties:
  "0":
    name: file.link
    label: Spec
  "1":
    name: type
    label: Type
  "2":
    name: date
    label: Date
  "3":
    name: created_by
    label: Created By
  "4":
    name: file.mtime
    label: Modified
views:
  - type: table
    name: All Specs
    order:
      - type
      - file.name
      - file.mtime
      - file.backlinks
    sort:
      - property: file.mtime
        direction: DESC
      - property: type
        direction: ASC
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
