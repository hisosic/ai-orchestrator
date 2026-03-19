# Task 1: 클러스터 제거 버튼 → 실제 컨테이너 종료

## 변경 사항
- `handleClusterDeleteNode`: 노드 제거 시 해당 노드의 모든 컨테이너를 먼저 종료 후 제거
- 새 엔드포인트 `POST /v1/cluster/container/stop`: 마스터를 통해 원격 노드 컨테이너 종료
- 새 엔드포인트 `DELETE /v1/cluster/container/{id}?node=name`: 마스터 프록시 삭제
- 대시보드: 컨테이너 개별 삭제 시 마스터 API 경유 (직접 노드 연결 → 마스터 프록시)

## 수정 파일
- internal/server/server.go
- static/dashboard.html
