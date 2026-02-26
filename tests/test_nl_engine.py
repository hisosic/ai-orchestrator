"""Tests for natural language intent parsing."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from orchestrator.models import IntentAction
from orchestrator.nl_engine import parse


def test_scale_korean():
    intent = parse("nginx를 5개로 스케일해줘")
    assert intent.action == IntentAction.SCALE
    assert intent.service_name == "nginx"
    assert intent.replicas == 5


def test_scale_english():
    intent = parse("scale nginx to 3")
    assert intent.action == IntentAction.SCALE
    assert intent.service_name == "nginx"
    assert intent.replicas == 3


def test_deploy_simple():
    intent = parse("nginx 배포해줘")
    assert intent.action == IntentAction.DEPLOY
    assert intent.service_name == "nginx"


def test_deploy_with_image():
    intent = parse("webapp 배포 이미지 myapp:v1")
    assert intent.action == IntentAction.DEPLOY
    assert intent.service_name == "webapp"
    assert intent.image == "myapp:v1"


def test_resource_memory():
    intent = parse("redis 메모리 512m")
    assert intent.action == IntentAction.RESOURCE
    assert intent.service_name == "redis"
    assert intent.memory == "512m"


def test_resource_memory_korean():
    intent = parse("nginx에 메모리 256")
    assert intent.action == IntentAction.RESOURCE
    assert intent.service_name == "nginx"
    assert intent.memory == "256m"


def test_stop():
    intent = parse("nginx 중지해줘")
    assert intent.action == IntentAction.STOP
    assert intent.service_name == "nginx"


def test_stop_container_korean():
    """'{name} 컨테이너 종료해줘' 패턴"""
    intent = parse("nginx 컨테이너 종료해줘")
    assert intent.action == IntentAction.STOP
    assert intent.service_name == "nginx"
    intent2 = parse("redis 컨테이너 중지해줘")
    assert intent2.action == IntentAction.STOP
    assert intent2.service_name == "redis"


def test_list():
    intent = parse("서비스 목록")
    assert intent.action == IntentAction.LIST
    intent2 = parse("list services")
    assert intent2.action == IntentAction.LIST


def test_unknown():
    intent = parse("asdf qwer zxcv")
    assert intent.action == IntentAction.UNKNOWN
