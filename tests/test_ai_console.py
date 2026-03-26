"""Tests for AI engine and console."""
import os
import json
import pytest
from unittest.mock import Mock, patch, MagicMock

# Add src to path
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from orchestrator.models import IntentAction, ParsedIntent, CommandRequest
from orchestrator.nl_engine import parse as regex_parse


class TestRegexFallback:
    """Test regex-based NL parsing (fallback)."""

    def test_scale_command_korean(self):
        """Test Korean scale command."""
        result = regex_parse("nginx를 5개로 스케일해줘")
        assert result.action == IntentAction.SCALE
        assert result.service_name == "nginx"
        assert result.replicas == 5

    def test_scale_command_english(self):
        """Test English scale command."""
        result = regex_parse("scale redis to 3")
        assert result.action == IntentAction.SCALE
        assert result.service_name == "redis"
        assert result.replicas == 3

    def test_deploy_command_korean(self):
        """Test Korean deploy command."""
        result = regex_parse("webapp 배포해줘")
        assert result.action == IntentAction.DEPLOY
        assert result.service_name == "webapp"

    def test_deploy_with_image_korean(self):
        """Test Korean deploy with image."""
        result = regex_parse("webapp 배포 이미지 myapp:v1")
        assert result.action == IntentAction.DEPLOY
        assert result.service_name == "webapp"
        assert result.image == "myapp:v1"

    def test_resource_memory_korean(self):
        """Test Korean memory resource command."""
        result = regex_parse("redis 메모리 512m")
        assert result.action == IntentAction.RESOURCE
        assert result.service_name == "redis"
        assert result.memory == "512m"

    def test_stop_command_korean(self):
        """Test Korean stop command."""
        result = regex_parse("nginx 컨테이너 종료해줘")
        assert result.action == IntentAction.STOP
        assert result.service_name == "nginx"

    def test_list_command(self):
        """Test list command."""
        result = regex_parse("목록")
        assert result.action == IntentAction.LIST

    def test_unknown_command(self):
        """Test unknown command."""
        result = regex_parse("random gibberish")
        assert result.action == IntentAction.UNKNOWN


class TestAIEngine:
    """Test AI engine with Claude API."""

    @pytest.mark.skipif(
        not os.environ.get("ANTHROPIC_API_KEY"),
        reason="ANTHROPIC_API_KEY not set",
    )
    def test_ai_engine_import(self):
        """Test AI engine can be imported."""
        from orchestrator.ai_engine import AIEngine
        assert AIEngine is not None

    @patch("orchestrator.ai_engine.anthropic.Anthropic")
    def test_ai_parse_scale(self, mock_anthropic_class):
        """Test AI engine parsing scale command."""
        from orchestrator.ai_engine import AIEngine

        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.content = [MagicMock()]
        mock_response.content[0].text = json.dumps({
            "action": "scale",
            "service_name": "nginx",
            "replicas": 5,
        })
        mock_client.messages.create.return_value = mock_response
        mock_anthropic_class.return_value = mock_client

        engine = AIEngine(api_key="test-key")
        result = engine.parse("nginx를 5개로 스케일해줘")

        assert result.action == IntentAction.SCALE
        assert result.service_name == "nginx"
        assert result.replicas == 5

    @patch("orchestrator.ai_engine.anthropic.Anthropic")
    def test_ai_parse_deploy(self, mock_anthropic_class):
        """Test AI engine parsing deploy command."""
        from orchestrator.ai_engine import AIEngine

        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.content = [MagicMock()]
        mock_response.content[0].text = json.dumps({
            "action": "deploy",
            "service_name": "myapp",
            "image": "myapp:v1",
        })
        mock_client.messages.create.return_value = mock_response
        mock_anthropic_class.return_value = mock_client

        engine = AIEngine(api_key="test-key")
        result = engine.parse("myapp 배포해줘 이미지 myapp:v1")

        assert result.action == IntentAction.DEPLOY
        assert result.service_name == "myapp"
        assert result.image == "myapp:v1"

    @patch("orchestrator.ai_engine.anthropic.Anthropic")
    def test_ai_parse_memory_resource(self, mock_anthropic_class):
        """Test AI engine parsing memory resource command."""
        from orchestrator.ai_engine import AIEngine

        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.content = [MagicMock()]
        mock_response.content[0].text = json.dumps({
            "action": "resource",
            "service_name": "redis",
            "memory": "512m",
        })
        mock_client.messages.create.return_value = mock_response
        mock_anthropic_class.return_value = mock_client

        engine = AIEngine(api_key="test-key")
        result = engine.parse("redis에 메모리 512m 할당해줘")

        assert result.action == IntentAction.RESOURCE
        assert result.service_name == "redis"
        assert result.memory == "512m"

    @patch("orchestrator.ai_engine.anthropic.Anthropic")
    def test_ai_fallback_on_error(self, mock_anthropic_class):
        """Test AI engine falls back to regex on API error."""
        from orchestrator.ai_engine import AIEngine

        mock_anthropic_class.return_value.messages.create.side_effect = Exception(
            "API Error"
        )

        engine = AIEngine(api_key="test-key")
        result = engine.parse("nginx를 3개로")

        # Should eventually fall back to regex or return unknown
        assert result.action in (IntentAction.SCALE, IntentAction.UNKNOWN)

    @patch("orchestrator.ai_engine.anthropic.Anthropic")
    def test_ai_chat_mode(self, mock_anthropic_class):
        """Test AI chat mode."""
        from orchestrator.ai_engine import AIEngine

        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.content = [MagicMock()]
        mock_response.content[0].text = "현재 nginx는 3개 인스턴스로 실행 중입니다."
        mock_client.messages.create.return_value = mock_response
        mock_anthropic_class.return_value = mock_client

        engine = AIEngine(api_key="test-key")
        response = engine.chat("nginx 상태가 어떻게 되나요?")

        assert "nginx" in response or "3" in response

    @patch("orchestrator.ai_engine.anthropic.Anthropic")
    def test_ai_json_parsing_with_markdown(self, mock_anthropic_class):
        """Test AI engine can parse JSON wrapped in markdown."""
        from orchestrator.ai_engine import AIEngine

        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.content = [MagicMock()]
        mock_response.content[0].text = """```json
{
  "action": "scale",
  "service_name": "nginx",
  "replicas": 5
}
```"""
        mock_client.messages.create.return_value = mock_response
        mock_anthropic_class.return_value = mock_client

        engine = AIEngine(api_key="test-key")
        result = engine.parse("scale command")

        assert result.action == IntentAction.SCALE
        assert result.replicas == 5

    @patch("orchestrator.ai_engine.anthropic.Anthropic")
    def test_ai_analyze_and_advise(self, mock_anthropic_class):
        """Test AI advisor analysis."""
        from orchestrator.ai_engine import AIEngine

        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.content = [MagicMock()]
        mock_response.content[0].text = "[상태] 클러스터 정상\n[권장사항] 없음"
        mock_client.messages.create.return_value = mock_response
        mock_anthropic_class.return_value = mock_client

        engine = AIEngine(api_key="test-key")
        result = engine.analyze_and_advise({"services": [], "health_summary": {"total": 0, "running": 0, "stopped": 0}})

        assert "상태" in result

    @patch("orchestrator.ai_engine.anthropic.Anthropic")
    def test_ai_auto_decide(self, mock_anthropic_class):
        """Test AI auto-decision."""
        from orchestrator.ai_engine import AIEngine

        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.content = [MagicMock()]
        mock_response.content[0].text = json.dumps({
            "should_act": True,
            "action": "scale",
            "params": {"service_name": "nginx", "replicas": 3},
            "reason": "CPU 사용률 높음",
            "urgency": "high",
        })
        mock_client.messages.create.return_value = mock_response
        mock_anthropic_class.return_value = mock_client

        engine = AIEngine(api_key="test-key")
        decision = engine.auto_decide("CPU 높음", {"services": []})

        assert decision is not None
        assert decision["action"] == "scale"
        assert decision["urgency"] == "high"


class TestConsole:
    """Test console interface."""

    @patch("orchestrator.console.httpx.Client")
    def test_console_initialization(self, mock_http_client):
        """Test console initializes properly."""
        from orchestrator.console import OrchestrationConsole

        console = OrchestrationConsole(
            api_url="http://localhost:8000",
            use_ai=False,
        )
        assert console.api_url == "http://localhost:8000"
        assert console.use_ai is False

    @patch("orchestrator.console.httpx.Client")
    def test_console_help(self, mock_http_client):
        """Test console help display."""
        from orchestrator.console import OrchestrationConsole

        console = OrchestrationConsole(use_ai=False)
        # Should not raise
        console._show_help()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
