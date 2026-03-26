"""AI-powered natural language engine using Claude API for container command parsing and real-time operations.

Supports tool_use for direct container control: Claude can call orchestrator APIs
(deploy, scale, stop, migrate, status) as tools during conversation."""
import json
import logging
import os
import httpx
from typing import Optional, Dict, Any, List

import anthropic

from .models import IntentAction, ParsedIntent

logger = logging.getLogger("ai_engine")


def _get_api_key() -> Optional[str]:
    """Get Anthropic API key from environment."""
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        key = os.environ.get("CLAUDE_API_KEY")
    return key


# Tools that Claude can call to control containers
CONTAINER_TOOLS = [
    {
        "name": "cluster_deploy",
        "description": "Deploy a service across the cluster. Pulls the image and runs containers on scheduled nodes.",
        "input_schema": {
            "type": "object",
            "properties": {
                "image": {"type": "string", "description": "Docker image (e.g. nginx:alpine, redis:latest)"},
                "name": {"type": "string", "description": "Service name"},
                "replicas": {"type": "integer", "description": "Number of replicas", "default": 1},
                "strategy": {"type": "string", "enum": ["spread", "binpack", "least-loaded"], "default": "spread"},
            },
            "required": ["image"],
        },
    },
    {
        "name": "cluster_scale",
        "description": "Scale a running service up or down to the specified number of replicas.",
        "input_schema": {
            "type": "object",
            "properties": {
                "service_name": {"type": "string", "description": "Service name to scale"},
                "replicas": {"type": "integer", "description": "Target replica count"},
            },
            "required": ["service_name", "replicas"],
        },
    },
    {
        "name": "cluster_stop",
        "description": "Stop and remove all containers for a service across all nodes.",
        "input_schema": {
            "type": "object",
            "properties": {
                "service_name": {"type": "string", "description": "Service name to stop"},
            },
            "required": ["service_name"],
        },
    },
    {
        "name": "cluster_status",
        "description": "Get full cluster status: nodes, services, resource usage, alerts.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "list_services",
        "description": "List all managed services with their status, replicas, and endpoints.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "cluster_migrate",
        "description": "Migrate a container from one node to another (preserves state).",
        "input_schema": {
            "type": "object",
            "properties": {
                "container_id": {"type": "string", "description": "Container ID to migrate"},
                "source_node": {"type": "string", "description": "Source node name"},
                "destination_node": {"type": "string", "description": "Destination node name"},
                "service_name": {"type": "string", "description": "Service name (optional)"},
            },
            "required": ["container_id", "source_node", "destination_node"],
        },
    },
    {
        "name": "set_resource_limits",
        "description": "Set memory or CPU limits for a service. Applies on next scale/deploy.",
        "input_schema": {
            "type": "object",
            "properties": {
                "service_name": {"type": "string"},
                "memory": {"type": "string", "description": "Memory limit (e.g. 512m, 1g)"},
                "cpu": {"type": "string", "description": "CPU limit (e.g. 0.5, 2)"},
            },
            "required": ["service_name"],
        },
    },
]


def _execute_tool(tool_name: str, tool_input: dict) -> str:
    """Execute a tool by calling the local orchestrator API."""
    base = "http://localhost:8000"
    try:
        with httpx.Client(timeout=120) as client:
            if tool_name == "cluster_deploy":
                r = client.post(f"{base}/v1/cluster/deploy", json=tool_input)
            elif tool_name == "cluster_scale":
                r = client.post(f"{base}/v1/cluster/scale", json=tool_input)
            elif tool_name == "cluster_stop":
                r = client.post(f"{base}/v1/cluster/stop", json=tool_input)
            elif tool_name == "cluster_status":
                r = client.get(f"{base}/v1/cluster/status")
            elif tool_name == "list_services":
                r = client.get(f"{base}/v1/services")
            elif tool_name == "cluster_migrate":
                r = client.post(f"{base}/v1/cluster/migrate", json=tool_input)
            elif tool_name == "set_resource_limits":
                svc = tool_input.get("service_name", "")
                r = client.post(f"{base}/v1/command", json={
                    "command": f"{svc} 메모리 {tool_input.get('memory', '')} cpu {tool_input.get('cpu', '')}".strip()
                })
            else:
                return json.dumps({"error": f"Unknown tool: {tool_name}"})
            return r.text
    except Exception as e:
        return json.dumps({"error": str(e)})


class AIEngine:
    """AI-powered natural language parser and container advisor using Claude.

    The chat() method uses tool_use so Claude can directly call orchestrator APIs
    to deploy, scale, stop, migrate containers during conversation."""

    MODEL = "claude-sonnet-4-20250514"

    def __init__(self, api_key: Optional[str] = None):
        """Initialize AI engine with Anthropic client."""
        self.api_key = api_key or _get_api_key()
        if not self.api_key:
            raise ValueError(
                "ANTHROPIC_API_KEY environment variable must be set"
            )
        self.client = anthropic.Anthropic(api_key=self.api_key)
        self.conversation_history: List[Dict] = []

    def parse(self, command: str) -> ParsedIntent:
        """Parse natural language command into ParsedIntent using Claude.

        Args:
            command: Natural language command in Korean or English

        Returns:
            ParsedIntent with parsed action and parameters
        """
        raw = command.strip()
        if not raw:
            return ParsedIntent(action=IntentAction.UNKNOWN, raw=raw)

        try:
            prompt = self._build_prompt(raw)
            response = self.client.messages.create(
                model=self.MODEL,
                max_tokens=500,
                system="You are a container orchestration command parser. "
                       "Extract intent and parameters from user commands related to Docker containers. "
                       "Always respond with valid JSON format only. No markdown, no explanation.",
                messages=[
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
            )

            response_text = response.content[0].text
            return self._parse_response(response_text, raw)

        except anthropic.APIError as e:
            print(f"Claude API Error: {e}")
            from .nl_engine import parse as regex_parse
            return regex_parse(raw)
        except Exception as e:
            print(f"AI Engine Error: {e}")
            return ParsedIntent(action=IntentAction.UNKNOWN, raw=raw)

    def _build_prompt(self, command: str) -> str:
        """Build prompt for AI model."""
        return f"""Parse the following container orchestration command and extract the intent and parameters.

Supported actions: {", ".join([a.value for a in IntentAction])}

User command: "{command}"

Extract and respond with JSON containing:
- action: (scale, deploy, resource, stop, list, migrate, drain, cluster_status, node_list, or unknown)
- service_name: (the container/service name if applicable)
- replicas: (number of replicas for scale action)
- image: (image name/tag for deploy action)
- memory: (memory limit like "512m" or "1g" for resource action)
- cpu: (CPU limit like "0.5" or "2" for resource action)
- target_node: (target node name for migrate/drain actions)

Respond ONLY with valid JSON, no markdown or extra text."""

    def _parse_response(self, response_text: str, raw: str) -> ParsedIntent:
        """Parse AI response and create ParsedIntent."""
        try:
            response_text = response_text.strip()
            if response_text.startswith("```json"):
                response_text = response_text[7:]
            if response_text.startswith("```"):
                response_text = response_text[3:]
            if response_text.endswith("```"):
                response_text = response_text[:-3]

            data = json.loads(response_text)

            action_str = (data.get("action") or "").lower().strip()
            try:
                action = IntentAction(action_str)
            except ValueError:
                action = IntentAction.UNKNOWN

            intent = ParsedIntent(
                action=action,
                raw=raw,
                service_name=data.get("service_name"),
                replicas=data.get("replicas"),
                image=data.get("image"),
                memory=data.get("memory"),
                cpu=data.get("cpu"),
                target_node=data.get("target_node"),
            )

            if action == IntentAction.UNKNOWN:
                return intent
            if action == IntentAction.SCALE and (
                not intent.service_name or intent.replicas is None
            ):
                return ParsedIntent(action=IntentAction.UNKNOWN, raw=raw)
            if action == IntentAction.DEPLOY and not (
                intent.service_name or intent.image
            ):
                return ParsedIntent(action=IntentAction.UNKNOWN, raw=raw)

            return intent

        except json.JSONDecodeError:
            return ParsedIntent(action=IntentAction.UNKNOWN, raw=raw)
        except Exception as e:
            print(f"Error parsing AI response: {e}")
            return ParsedIntent(action=IntentAction.UNKNOWN, raw=raw)

    def chat(self, user_message: str, context: Optional[Dict[str, Any]] = None) -> str:
        """Interactive chat with tool_use — Claude can directly control containers.

        Claude sees the current cluster state and can call orchestrator APIs
        (deploy, scale, stop, migrate, status) as tools during the conversation.
        Tool results are fed back so Claude can summarize what happened.
        """
        try:
            system_prompt = self._build_system_prompt(context)
            self.conversation_history.append({"role": "user", "content": user_message})

            # Keep last 20 messages
            messages = self.conversation_history[-20:]

            # Agentic loop: keep calling Claude until no more tool_use
            max_turns = 5
            final_text = ""
            tool_results_log = []

            for _ in range(max_turns):
                response = self.client.messages.create(
                    model=self.MODEL,
                    max_tokens=2000,
                    system=system_prompt,
                    messages=messages,
                    tools=CONTAINER_TOOLS,
                    temperature=0.3,
                )

                # Collect text and tool_use blocks
                text_parts = []
                tool_uses = []
                for block in response.content:
                    if block.type == "text":
                        text_parts.append(block.text)
                    elif block.type == "tool_use":
                        tool_uses.append(block)

                if text_parts:
                    final_text = "\n".join(text_parts)

                if not tool_uses:
                    # No more tools to call — done
                    break

                # Execute each tool and build tool_result messages
                assistant_content = response.content
                messages.append({"role": "assistant", "content": assistant_content})

                tool_result_blocks = []
                for tu in tool_uses:
                    logger.info(f"AI tool_use: {tu.name}({json.dumps(tu.input, ensure_ascii=False)[:200]})")
                    result_str = _execute_tool(tu.name, tu.input)
                    # Truncate large results
                    if len(result_str) > 4000:
                        result_str = result_str[:4000] + "... (truncated)"
                    tool_result_blocks.append({
                        "type": "tool_result",
                        "tool_use_id": tu.id,
                        "content": result_str,
                    })
                    tool_results_log.append({"tool": tu.name, "input": tu.input, "result_preview": result_str[:200]})

                messages.append({"role": "user", "content": tool_result_blocks})

            # Save final assistant message to history
            self.conversation_history.append({"role": "assistant", "content": final_text})

            # Append tool execution summary if any tools were called
            if tool_results_log:
                summary = "\n".join([f"  [{t['tool']}] {json.dumps(t['input'], ensure_ascii=False)[:100]}" for t in tool_results_log])
                logger.info(f"AI chat executed {len(tool_results_log)} tool(s):\n{summary}")

            return final_text
        except anthropic.APIError as e:
            return f"API 오류: {e}"
        except Exception as e:
            logger.error(f"AI chat error: {e}", exc_info=True)
            return f"오류: {e}"

    def analyze_and_advise(self, cluster_context: Dict[str, Any]) -> str:
        """Analyze current cluster state and provide operational advice.

        Real-time analysis of container health, resource usage, and recommendations.

        Args:
            cluster_context: Dict with services, containers, resources, alerts info

        Returns:
            Analysis and recommendations in Korean
        """
        try:
            system_prompt = """You are an expert container operations advisor.
Analyze the current cluster state and provide actionable recommendations.
Respond in Korean. Be concise and specific.
Focus on:
1. Health issues - containers that are down or unhealthy
2. Resource optimization - over/under-provisioned services
3. Scaling recommendations based on resource usage
4. Security concerns
5. Specific commands the user can run to fix issues

Format your response with clear sections using markers like [상태], [권장사항], [명령어]."""

            context_str = json.dumps(cluster_context, ensure_ascii=False, indent=2, default=str)
            user_message = f"""현재 클러스터 상태를 분석하고 운영 권장사항을 알려주세요:

{context_str}"""

            response = self.client.messages.create(
                model=self.MODEL,
                max_tokens=2000,
                system=system_prompt,
                messages=[{"role": "user", "content": user_message}],
                temperature=0.5,
            )
            return response.content[0].text
        except anthropic.APIError as e:
            return f"분석 API 오류: {e}"
        except Exception as e:
            return f"분석 오류: {e}"

    def auto_decide(self, event: str, cluster_context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """AI-driven auto-decision for container events.

        Given an event (e.g., container crash, high CPU), decide what action to take.

        Args:
            event: Description of the event
            cluster_context: Current cluster state

        Returns:
            Dict with 'action', 'params', 'reason' or None if no action needed
        """
        try:
            system_prompt = """You are an autonomous container operations engine.
Given a cluster event, decide what action to take automatically.
Respond ONLY with valid JSON. No markdown, no explanation outside JSON.

Response format:
{
  "should_act": true/false,
  "action": "scale|deploy|resource|stop|none",
  "params": {
    "service_name": "...",
    "replicas": N,
    "memory": "...",
    "cpu": "..."
  },
  "reason": "Brief explanation in Korean",
  "urgency": "high|medium|low"
}

Rules:
- Only recommend action if clearly beneficial
- For crashes: restart or scale up
- For high CPU (>80%): scale up or increase CPU limit
- For high memory (>85%): increase memory limit
- For idle services (CPU<5%, no traffic): suggest scale down
- Be conservative - prefer no action over risky changes"""

            context_str = json.dumps(cluster_context, ensure_ascii=False, indent=2, default=str)
            user_message = f"""이벤트: {event}

클러스터 상태:
{context_str}"""

            response = self.client.messages.create(
                model=self.MODEL,
                max_tokens=500,
                system=system_prompt,
                messages=[{"role": "user", "content": user_message}],
                temperature=0.2,
            )

            response_text = response.content[0].text.strip()
            if response_text.startswith("```"):
                response_text = response_text.split("\n", 1)[-1]
            if response_text.endswith("```"):
                response_text = response_text[:-3]

            decision = json.loads(response_text)
            if decision.get("should_act"):
                return decision
            return None

        except (json.JSONDecodeError, anthropic.APIError, Exception) as e:
            print(f"Auto-decision error: {e}")
            return None

    def _build_system_prompt(self, context: Optional[Dict[str, Any]] = None) -> str:
        """Build system prompt with cluster context."""
        prompt = """You are a helpful container orchestration assistant powered by Claude.
You help users manage Docker containers and services in real-time.
Respond in the same language as the user (Korean or English).

You can help with:
- Deploying and scaling containers
- Managing resources (memory, CPU)
- Monitoring container status and health
- Migrating containers between nodes
- Analyzing cluster state and providing recommendations
- Auto-healing and optimization suggestions

When you identify issues, proactively suggest solutions with specific commands.
When appropriate, suggest using natural language commands."""

        if context:
            if context.get("services"):
                services_info = []
                for s in context["services"]:
                    name = s.get("name", "?")
                    replicas = s.get("replicas", 0)
                    status = s.get("status", "unknown")
                    mem = s.get("memory_limit", "-")
                    cpu = s.get("cpu_limit", "-")
                    services_info.append(f"  {name}: {replicas}개 ({status}) [mem:{mem}, cpu:{cpu}]")
                prompt += f"\n\n현재 서비스:\n" + "\n".join(services_info)
            if context.get("nodes"):
                nodes_str = ", ".join([n.get("name", "") for n in context["nodes"]])
                prompt += f"\n클러스터 노드: {nodes_str}"
            if context.get("alerts"):
                alerts_str = "\n".join([f"  - {a}" for a in context["alerts"][:5]])
                prompt += f"\n활성 알림:\n{alerts_str}"

        return prompt
