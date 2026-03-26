"""Interactive console for container orchestration with Claude AI tool_use.

All natural language input is sent to /v1/ai/chat which uses Claude tool_use
to both understand intent and execute container operations directly.

Uses only Python standard library (no pip dependencies required)."""
import json
import os
import sys
import threading
import urllib.request
import urllib.error


class OrchestrationConsole:
    """Interactive console for container management with Claude AI tool_use."""

    def __init__(self, api_url="http://localhost:8000", api_token=None):
        self.api_url = api_url.rstrip("/")
        self.api_token = api_token or os.environ.get("ORCHESTRATOR_API_TOKEN")
        self.auto_advisor_running = False
        self._advisor_thread = None
        self._advisor_stop = threading.Event()

    def _http_get(self, path, timeout=30):
        """HTTP GET using stdlib."""
        url = self.api_url + path
        req = urllib.request.Request(url)
        if self.api_token:
            req.add_header("Authorization", f"Bearer {self.api_token}")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())

    def _http_post(self, path, data, timeout=120):
        """HTTP POST using stdlib."""
        url = self.api_url + path
        body = json.dumps(data).encode()
        req = urllib.request.Request(url, data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        if self.api_token:
            req.add_header("Authorization", f"Bearer {self.api_token}")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())

    def run(self):
        """Run interactive console."""
        self._show_welcome()
        self._check_ai_status()

        while True:
            try:
                command = input("\n> ").strip()
                if not command:
                    continue
                if command.lower() in ("exit", "quit", "q"):
                    self._stop_advisor()
                    print("Goodbye!")
                    break
                if command.lower() in ("help", "h", "?"):
                    self._show_help()
                    continue
                if command.lower() in ("status", "info"):
                    self._ai_chat("클러스터 상태와 서비스 목록을 간단히 요약해줘")
                    continue
                if command.lower() == "advisor":
                    self._ai_chat("클러스터 상태를 분석하고 최적화 권장사항을 알려줘")
                    continue
                if command.lower() == "advisor on":
                    self._start_advisor()
                    continue
                if command.lower() == "advisor off":
                    self._stop_advisor()
                    continue

                self._ai_chat(command)
            except KeyboardInterrupt:
                self._stop_advisor()
                print("\n\nGoodbye!")
                break
            except Exception as e:
                print(f"[ERROR] {e}")

    def _show_welcome(self):
        print()
        print("=" * 60)
        print("  AI Container Orchestrator - NL Command Console")
        print("  Powered by Claude API (tool_use)")
        print("=" * 60)
        print(f"  API: {self.api_url}")
        print()
        print("  Type 'help' for examples, 'exit' to quit.")
        print()

    def _check_ai_status(self):
        try:
            data = self._http_get("/v1/ai/status", timeout=5)
            if data.get("ai_enabled"):
                print("  [OK] AI Engine: Active (Claude)")
            else:
                print("  [WARN] AI Engine: Disabled - ANTHROPIC_API_KEY not set")
        except Exception:
            print("  [WARN] Cannot reach server.")
        print()

    def _show_help(self):
        print("""
Commands:
  help              Show this help
  status            Show cluster & service status
  advisor           Run cluster analysis
  advisor on/off    Start/stop continuous monitoring
  exit              Exit console

Natural Language Examples:
  nginx를 3개로 배포해줘          Deploy nginx with 3 replicas
  redis 서비스 스케일 5개로        Scale redis to 5
  httpd 서비스 중지해             Stop httpd service
  클러스터 상태 알려줘             Show cluster status
  서비스 목록 보여줘              List all services
  nginx 메모리 512m으로 설정해     Set memory limit
  컨테이너를 node-b로 옮겨줘       Migrate container
""")

    def _ai_chat(self, message):
        """Send natural language to /v1/ai/chat and display results."""
        try:
            data = self._http_post("/v1/ai/chat", {"message": message}, timeout=120)

            # Show executed actions
            actions = data.get("actions", [])
            if actions:
                print()
                print("-" * 40)
                print(f"  Executed {len(actions)} action(s):")
                for a in actions:
                    tool = a.get("tool", "?")
                    inp = a.get("input", {})
                    if isinstance(inp, str):
                        try:
                            inp = json.loads(inp)
                        except Exception:
                            pass
                    inp_str = json.dumps(inp, ensure_ascii=False) if isinstance(inp, dict) else str(inp)
                    if len(inp_str) > 80:
                        inp_str = inp_str[:80] + "..."
                    print(f"    [{tool}] {inp_str}")
                print("-" * 40)

            # Show Claude's response
            response = data.get("response", "")
            if response:
                print()
                print(response)
            elif not actions:
                print("[WARN] No response from AI.")

        except urllib.error.URLError as e:
            print(f"[ERROR] Cannot connect to {self.api_url}: {e.reason}")
            self._fallback_command(message)
        except Exception as e:
            print(f"[ERROR] {e}")

    def _fallback_command(self, command):
        """Fallback to regex-based /v1/command when AI is unavailable."""
        try:
            result = self._http_post("/v1/command", {"command": command, "dry_run": False}, timeout=30)
            if result.get("success"):
                print(f"[OK] {result.get('message', '')}")
            else:
                print(f"[FAIL] {result.get('message', '')}")
        except Exception as e:
            print(f"[ERROR] Fallback also failed: {e}")

    def _start_advisor(self):
        if self.auto_advisor_running:
            print("[WARN] Advisor is already running.")
            return

        self.auto_advisor_running = True
        self._advisor_stop.clear()

        def advisor_loop():
            while not self._advisor_stop.is_set():
                try:
                    data = self._http_post(
                        "/v1/ai/chat",
                        {"message": "클러스터 상태를 점검하고, 이상이 있으면 조치 방안을 알려줘. 문제가 없으면 간단히 정상이라고만 말해."},
                        timeout=120,
                    )
                    actions = data.get("actions", [])
                    response = data.get("response", "")
                    if actions or ("정상" not in response and response):
                        print(f"\n[ADVISOR] {response}")
                        for a in actions:
                            print(f"  -> [{a.get('tool')}] executed")
                except Exception as e:
                    print(f"\n[ADVISOR ERROR] {e}")
                self._advisor_stop.wait(60)
            self.auto_advisor_running = False

        self._advisor_thread = threading.Thread(target=advisor_loop, daemon=True)
        self._advisor_thread.start()
        print("[OK] Real-time advisor started (60s interval)")

    def _stop_advisor(self):
        if self.auto_advisor_running:
            self._advisor_stop.set()
            self.auto_advisor_running = False
            print("[OK] Real-time advisor stopped.")

    def close(self):
        self._stop_advisor()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="AI Container Orchestrator - NL Command Console")
    parser.add_argument("--api", default=os.environ.get("ORCHESTRATOR_API_URL", "http://localhost:8000"))
    parser.add_argument("--token", default=os.environ.get("ORCHESTRATOR_API_TOKEN"))
    parser.add_argument("--command", "-c", type=str, help="Execute single NL command and exit")
    args = parser.parse_args()

    try:
        console = OrchestrationConsole(api_url=args.api, api_token=args.token)
        if args.command:
            console._ai_chat(args.command)
        else:
            console.run()
        console.close()
    except KeyboardInterrupt:
        print("\nGoodbye!")
        sys.exit(0)
    except Exception as e:
        print(f"Fatal error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
