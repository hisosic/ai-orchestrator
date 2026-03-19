# Task 2: Blockchain QuickStart - Service Name & Endpoint

## Summary
Added a "service name" input field to the Blockchain QuickStart card and added admin chain endpoint info to each node's status response.

## Changes Made

### 1. Dashboard HTML (`static/dashboard.html`)
- Added `qsBlockchainServiceName` text input (default: "blockchain") in the Blockchain QuickStart card, before the validators/citizens inputs (line ~429)
- Updated `renderBlockchainNodesInModal()` to include an "Endpoint" column showing the admin chain URL as a clickable link
- Updated `renderBlockchainNodes()` to include an "Endpoint" column similarly

### 2. Dashboard JS (within `static/dashboard.html`)
- Regular quickstart (`btnQsBlockchain` click handler): reads `qsBlockchainServiceName` and passes `service_name` in the API request body
- Distributed quickstart (`btnQsBlockchainDist` click handler): same addition

### 3. Go Server (`internal/server/server.go`)
- `handleQuickstartBlockchain` (line ~544): Added `ServiceName string` field to request struct, default "blockchain", passed as `QS_SERVICE_NAME` env var to quickstart script
- `handleQuickstartBlockchainDistributed` (line ~3592): Added `ServiceName string` field to request struct, default "blockchain"
- `handleQuickstartBlockchainStatus` (line ~714): For each running node, queries `http://{container_name}:9080/admin/chain` to get chain list, extracts NID from the first chain, and builds endpoint URL as `http://{container_name}:9080/admin/chain/{NID}`. This endpoint is returned in the `endpoint` field of each node object.

## Endpoint Format
```
http://{container_name}:9080/admin/chain/{NID}
```
Where NID is dynamically fetched from the goloop admin API (`GET /admin/chain` returns an array of chain objects, each with a `nid` field).

## Environment Variable
- `QS_SERVICE_NAME` - passed to `quickstart.sh` for use in container naming or other service identification
