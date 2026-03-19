# Task 5: Command Console Preview (명령 콘솔 자연어 기능 개선)

## Summary
Enhanced the command console to show a preview of parsed intent and equivalent CLI command before executing.

## Changes in `static/dashboard.html`

### CSS (line ~111)
- `.command-preview` - styled preview box with accent background
- `.preview-label`, `.preview-intent`, `.preview-cli`, `.preview-actions` - sub-elements

### HTML (command console section)
- Added "미리보기" button (`#btnPreview`) next to "실행"
- Added `#commandPreview` div with:
  - `#previewIntent` - shows parsed action, target, replicas, etc.
  - `#previewCli` - shows equivalent `curl` command
  - "확인 후 실행" (`#btnPreviewConfirm`) and "취소" (`#btnPreviewCancel`) buttons

### JavaScript
- `previewCommand()` - calls `POST /v1/command` with `dry_run: true`, displays intent + CLI
- `runCommand()` - now calls dry_run first to show preview, then waits for user confirm
- `executeCommand(cmd, dryRun)` - actual execution after confirmation
- `buildCliFromIntent(data)` - generates equivalent curl command from parsed intent
- `formatIntent(data)` - formats intent fields for display
- `pendingPreviewCmd` - stores command awaiting confirmation

## Flow
1. User types natural language command
2. Click "미리보기" or "실행" -> dry_run API call
3. Preview box shows: action, target, replicas, equivalent curl command
4. User clicks "확인 후 실행" to execute or "취소" to abort
