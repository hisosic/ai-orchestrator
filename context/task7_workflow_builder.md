# Task 7: Workflow Builder (워크플로 빌더 - 순차 기동)

## Summary
Added a workflow builder in the QuickStart panel where users can drag container images to create a startup sequence, reorder them, and execute containers sequentially with visual progress.

## Changes in `static/dashboard.html`

### CSS (line ~137)
- `.workflow-builder` - section container with top border
- `.wf-palette` - flex row of draggable image items
- `.wf-drag-item` - pill style draggable items (accent colored)
- `.wf-drop-zone` - dashed drop target with placeholder
- `.wf-step` - step row with number, name, image input, replicas input, status, remove button
- `.wf-step-num` - circular numbered badge
- `.wf-step-status` - status text with color classes: `.waiting`, `.running`, `.done`, `.failed`

### HTML (inside QuickStart `<details>` panel)
- `.workflow-builder` div with title "워크플로 빌더 (순차 기동)"
- `#wfPalette` with 4 draggable items: goloop, nginx, redis, custom image
- `#wfDropZone` - drop target for steps
- "순차 기동" (`#btnWfRun`) and "초기화" (`#btnWfClear`) buttons

### JavaScript (IIFE `initWorkflowBuilder`)
- Palette items carry `application/json` data (image + name) via drag
- Drop handler creates step rows with editable image path and replicas
- Steps support drag-to-reorder within the drop zone
- "순차 기동" executes each step in order via `POST /v1/containers/run`:
  - Updates status: 대기 -> 실행중 -> 완료/실패
  - Logs each step result to command log
- "초기화" clears all steps

## Palette Items
| Item | Default Image | Description |
|------|--------------|-------------|
| goloop | iconloop/goloop-icon | Blockchain node |
| nginx | nginx:latest | Web server |
| redis | redis:latest | Cache/DB |
| custom | (empty - user fills in) | Any Docker image |

## Status Flow
`대기` (waiting) -> `실행중...` (running) -> `완료` (done) / `실패` (failed)
