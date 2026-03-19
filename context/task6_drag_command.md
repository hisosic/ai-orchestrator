# Task 6: Drag & Drop Command Blocks (드래그앤드랍 조합 실행)

## Summary
Added a drag-and-drop command builder in the command console. Users drag action blocks into a composition area, fill in parameters, then execute all commands in sequence.

## Changes in `static/dashboard.html`

### CSS (line ~119)
- `.cmd-block-area` - container for block palette and compose zone
- `.cmd-blocks-palette` - flex row of draggable blocks
- `.cmd-block` - pill/chip style with per-action colors (deploy=green, scale=blue, stop=red, migrate=purple, status=yellow)
- `.cmd-compose-zone` - dashed drop target area
- `.compose-item` - dropped item with input fields
- `.cmd-compose-actions` - action buttons row

### HTML (inside command console panel-body)
- `.cmd-block-area` with 5 draggable blocks: 배포, 스케일, 중지, 마이그레이션, 상태확인
- `#cmdComposeZone` - drop target
- "조합 실행" (`#btnComposeRun`) and "초기화" (`#btnComposeClear`) buttons

### JavaScript (IIFE `initCmdBlocks`)
- `blockForms` object defines input fields per action type
- Drag events on palette blocks set `dataTransfer` with action name
- Drop handler creates form items in compose zone with appropriate inputs
- "조합 실행" iterates through composed items, builds natural language command for each, calls `POST /v1/command` sequentially
- Each item border color updates to green (success) or red (failure)
- "초기화" clears all composed items

## Supported Actions
| Block | Inputs | Generated Command |
|-------|--------|-------------------|
| 배포 | image, replicas | `{image} {replicas}개 배포해줘` |
| 스케일 | service, replicas | `{service} {replicas}개로 스케일해줘` |
| 중지 | service | `{service} 중지해줘` |
| 마이그레이션 | container, target_node | `{container} {node}로 마이그레이션해줘` |
| 상태확인 | (none) | `서비스 목록` |
