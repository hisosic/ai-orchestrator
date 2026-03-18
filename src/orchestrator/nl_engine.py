"""Natural language intent parsing (Korean + English) with cluster support."""
import re
from .models import IntentAction, ParsedIntent


# Patterns: (regex, action, group names for service/replicas/image/memory/cpu)
# Order matters: more specific patterns first
PATTERNS = [
    # Scale: "nginx를 5개로 스케일해줘", "scale nginx to 5"
    (r"(?:스케일|스케일링|늘려|줄여).*?(\w+).*?(\d+)\s*개", IntentAction.SCALE, ("service", "replicas")),
    (r"(\w+)\s*(?:를|을)\s*(\d+)\s*개(?:로)?\s*(?:스케일|실행)", IntentAction.SCALE, ("service", "replicas")),
    (r"(?:scale|replicate)\s+(\w+)\s+(?:to|up to)\s*(\d+)", IntentAction.SCALE, ("service", "replicas")),
    (r"(\w+)\s*(\d+)\s*개\s*(?:로)?\s*스케일", IntentAction.SCALE, ("service", "replicas")),
    # Migrate: "nginx를 node-b로 마이그레이션해줘", "migrate nginx to node-b"
    (r"(\w[\w-]*)\s*(?:를|을)\s*(\w[\w-]*)\s*(?:로|으로)\s*(?:마이그레이션|이동|옮겨|이전)", IntentAction.MIGRATE, ("service", "target_node")),
    (r"(?:마이그레이션|이동|옮겨)\s+(\w[\w-]*)\s+(\w[\w-]*)", IntentAction.MIGRATE, ("service", "target_node")),
    (r"(?:migrate|move)\s+(\w[\w-]*)\s+(?:to|from\s+\w+\s+to)\s+(\w[\w-]*)", IntentAction.MIGRATE, ("service", "target_node")),
    # Drain: "node-a 드레인해줘", "drain node-a"
    (r"(\w[\w-]*)\s*(?:를|을)?\s*(?:드레인|비워|배출)", IntentAction.DRAIN, ("target_node", None)),
    (r"(?:drain|evacuate)\s+(\w[\w-]*)", IntentAction.DRAIN, ("target_node", None)),
    # Deploy: "webapp 배포 이미지 myapp:v1" before "X 배포", then "nginx 배포해줘", "deploy nginx"
    (r"(\w+)\s*배포.*?이미지\s+(\S+)", IntentAction.DEPLOY, ("name", "image")),
    (r"(\w+)\s*(?:서비스\s*)?(?:를\s*)?배포(?:\s*해줘)?", IntentAction.DEPLOY, ("name", None)),
    (r"deploy\s+(\w+)(?:\s+(\S+))?", IntentAction.DEPLOY, ("name", "image")),
    # Resource: "redis 메모리 512MB 제한", "nginx에 메모리 256"
    (r"([a-zA-Z0-9_-]+).*?메모리\s*(\d+)\s*(m|mb|g|gb)?", IntentAction.RESOURCE, ("service", "memory")),
    (r"([a-zA-Z0-9_-]+)\s*에\s*메모리\s*(\d+)\s*(m|mb|g|gb)?", IntentAction.RESOURCE, ("service", "memory")),
    (r"(?:memory|mem)\s+(?:of\s+)?(\w+)\s+(?:to\s+)?(\d+)\s*(m|mb|g|gb)?", IntentAction.RESOURCE, ("service", "memory")),
    (r"(\w+).*?cpu\s*(\d*\.?\d+)", IntentAction.RESOURCE, ("service", "cpu")),
    # Stop: "nginx 컨테이너 종료해줘", "nginx 중지", "stop redis"
    (r"([a-zA-Z0-9_-]+)\s*컨테이너\s*(?:종료|중지)(?:\s*해줘)?", IntentAction.STOP, ("service", None)),
    (r"(\w+)\s*(?:를|을)?\s*중지", IntentAction.STOP, ("service", None)),
    (r"stop\s+(\w+)", IntentAction.STOP, ("service", None)),
    # Cluster status
    (r"클러스터\s*(?:상태|상황|현황|정보)|cluster\s*(?:status|info)", IntentAction.CLUSTER_STATUS, ()),
    # Node list
    (r"노드\s*(?:목록|리스트|상태)|node[s]?\s*(?:list|status)", IntentAction.NODE_LIST, ()),
    # List
    (r"목록|리스트|서비스\s*목록|list\s*services?", IntentAction.LIST, ()),
]

# Normalize memory suffix to docker format (e.g. 512m, 1g)
def _norm_memory(num: str, unit: str) -> str:
    u = (unit or "m").lower().replace("mb", "m").replace("gb", "g")
    if u not in ("m", "g"):
        u = "m"
    return f"{num}{u}"


def parse(command: str) -> ParsedIntent:
    """Parse natural language command into ParsedIntent."""
    raw = command.strip()
    if not raw:
        return ParsedIntent(action=IntentAction.UNKNOWN, raw=raw)

    lower = raw.lower()
    for pattern, action, groups in PATTERNS:
        m = re.search(pattern, raw, re.IGNORECASE)
        if not m:
            continue
        gs = m.groups()
        intent = ParsedIntent(action=action, raw=raw)

        if "service" in groups:
            idx = groups.index("service")
            if idx < len(gs) and gs[idx]:
                intent.service_name = gs[idx].strip()
        if "replicas" in groups:
            idx = groups.index("replicas")
            if idx < len(gs) and gs[idx]:
                try:
                    intent.replicas = int(gs[idx])
                except ValueError:
                    pass
        if "name" in groups:
            idx = groups.index("name")
            if idx < len(gs) and gs[idx]:
                intent.service_name = gs[idx].strip()
        if "image" in groups:
            idx = groups.index("image")
            if idx < len(gs) and gs[idx]:
                intent.image = gs[idx].strip()
        if "memory" in groups and action == IntentAction.RESOURCE:
            if len(gs) >= 2 and gs[1]:
                intent.memory = _norm_memory(gs[1], gs[2] if len(gs) > 2 else "m")
        if "cpu" in groups:
            idx = groups.index("cpu")
            if idx < len(gs) and gs[idx]:
                intent.cpu = gs[idx].strip()
        if "target_node" in groups:
            idx = groups.index("target_node")
            if idx < len(gs) and gs[idx]:
                intent.target_node = gs[idx].strip()

        return intent

    # Fallback: single word might be "list" or service name for deploy
    words = raw.split()
    if len(words) == 1 and words[0].lower() in ("list", "목록", "리스트"):
        return ParsedIntent(action=IntentAction.LIST, raw=raw)
    if len(words) == 1 and words[0].lower() not in ("list", "목록"):
        return ParsedIntent(
            action=IntentAction.DEPLOY,
            service_name=words[0],
            image=None,
            raw=raw,
        )

    return ParsedIntent(action=IntentAction.UNKNOWN, raw=raw)
