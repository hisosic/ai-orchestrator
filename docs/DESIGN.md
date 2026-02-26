# AI Container Orchestrator - 설계 문서

## 개요

쿠버네티스와 유사한 컨테이너 오케스트레이션 환경이지만, **자연어**로 스케일링·리소스 제어·배포를 수행하는 오케스트레이터입니다.  
오케스트레이터 자체도 **컨테이너 기반**으로 기동하며, 확장이 쉬운 구조로 설계합니다.

## 목표

- **자연어 기반 제어**: "nginx를 5개로 스케일해줘", "redis 메모리 512MB로 제한해줘" 등
- **스케일링**: 레플리카 수 증가/감소
- **리소스 제어**: CPU/메모리 제한 설정
- **배포**: 이미지 기반 서비스 배포/업데이트
- **자체 컨테이너화**: 오케스트레이터를 Docker 컨테이너로 실행
- **확장 용이**: 플러그인/어댑터 구조로 런타임·NL 엔진 교체 가능

## 아키텍처

```
┌─────────────────────────────────────────────────────────────┐
│  Client (CLI / HTTP / Chat)                                  │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│  AI Orchestrator API (FastAPI)                               │
│  - POST /v1/command  (자연어 명령)                           │
│  - GET  /v1/services (서비스 목록)                           │
│  - GET  /v1/health   (헬스체크)                              │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│  NL Intent Engine                                            │
│  - 자연어 → Intent (scale / deploy / resource / stop 등)     │
│  - 패턴 매칭 + 선택적 LLM (OpenAI/Ollama)                    │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│  Container Runtime Adapter (Docker)                          │
│  - create/start/stop/scale containers                        │
│  - set resource limits (memory, cpu)                         │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│  Docker Socket / containerd (호스트 또는 sidecar)             │
└─────────────────────────────────────────────────────────────┘
```

## 핵심 컴포넌트

| 컴포넌트 | 역할 |
|----------|------|
| **API Server** | HTTP로 자연어 명령 수신, 결과 반환 |
| **NL Intent Engine** | 자연어 파싱 → 구조화된 Intent (action, target, params) |
| **Runtime Adapter** | Intent 실행 (Docker SDK로 컨테이너 생성/스케일/리소스 설정) |
| **State Store** | 서비스명·이미지·레플리카 수·리소스 설정 등 (JSON 파일 또는 DB) |

## 자연어 인텐트 예시

| 자연어 (예) | Intent | 파라미터 |
|-------------|--------|----------|
| nginx를 5개로 스케일해줘 | scale | service=nginx, replicas=5 |
| redis 메모리 512MB로 제한해줘 | resource | service=redis, memory=512m |
| webapp 서비스 배포해줘, 이미지 myapp:v1 | deploy | name=webapp, image=myapp:v1 |
| api 서비스 중지해줘 | stop | service=api |

## 배포 모델

- **Orchestrator**: 단일 Docker 이미지. Docker socket 마운트 또는 Docker-in-Docker로 호스트의 컨테이너 제어.
- **확장**: API를 여러 replica로 띄우고 로드밸런서 앞에 두면 수평 확장 가능. State는 공유 스토어(볼륨/DB) 사용.

## 기술 스택

- **Language**: Python 3.11+
- **API**: FastAPI
- **Container**: Docker SDK for Python
- **NL**: 정규식/키워드 기반 파서 + (선택) OpenAI API 또는 Ollama
