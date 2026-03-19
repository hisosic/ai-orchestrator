# Task 3: Unified Deploy Section

## Summary
Merged the two separate deploy sections in the dashboard's "이미지 / 배포" panel into one unified deploy workflow, and added distributed deploy (node selection) capability.

## Changes Made (static/dashboard.html)

### 1. Added distributed deploy checkbox + node selector (line ~636)
- Added a "분산 배포 (노드 선택)" checkbox (`#deployDistributed`) below the deploy button row
- When checked, shows a node selector (`#deployNodeSelector`) with checkboxes for each cluster node
- Node list is populated dynamically from `clusterNodes` (fetched from `/v1/cluster/nodes`)

### 2. Replaced `renderImages()` function (line ~1599)
- **Before**: Full deploy form with dropdown, replicas, advanced options (env, volumes, ports, user), and "기동"/"삭제" buttons
- **After**: Compact table showing image name, tags, size, with "선택" (select) and "삭제" (delete) buttons per row
- "선택" button fills the main deploy image input (`#deployImage`) and auto-fills service name (`#deployName`)
- Visual feedback: green border flash on image input when an image is selected
- Integrated image search filtering from `#imageSearchInput`

### 3. Updated deploy button handler (line ~2095)
- When `#deployDistributed` is checked, collects selected node names from `#deployNodeList` checkboxes
- Sends `nodes` array in the `/v1/cluster/deploy` API request body
- Backend already supports `nodes` field (see `handleClusterDeploy` in `internal/server/server.go:2317`)

### 4. Added event handlers (line ~2142)
- Distributed checkbox toggle: shows/hides node selector, populates node checkboxes
- `updateDeployNodeList()`: builds checkbox UI from `clusterNodes` + always includes 'local'
- Image search debounced input handler: triggers `loadAll()` after 300ms

## API Integration
- Deploy uses `POST /v1/cluster/deploy` with body: `{ image, name, replicas, strategy, nodes?, environment?, ports?, volumes?, memory?, cpu? }`
- The `nodes` field (string array) was already supported by the backend but not exposed in the UI until this change

## User Flow
1. User can type an image path directly OR click "선택" from the image list table
2. Service name auto-fills from the image repository name
3. User sets replicas, strategy, and optionally checks "분산 배포" to select specific nodes
4. Advanced options (env, ports, volumes, resources) available in collapsible details
5. Click "배포" to deploy via the unified cluster deploy API
