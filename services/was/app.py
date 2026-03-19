"""WAS - Simple application server that connects to Redis DB."""
import http.server
import json
import socket
import time
import os

DB_HOST = os.environ.get("DB_HOST", "orch-db-0")
DB_PORT = int(os.environ.get("DB_PORT", "6379"))
WAS_PORT = int(os.environ.get("WAS_PORT", "8080"))

def check_redis(host, port, timeout=2):
    """Check Redis connectivity by sending PING command."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((host, port))
        sock.send(b"*1\r\n$4\r\nPING\r\n")
        resp = sock.recv(64).decode().strip()
        sock.close()
        return resp == "+PONG"
    except Exception as e:
        return False

def resolve_host(host):
    """Try to resolve hostname."""
    try:
        ip = socket.gethostbyname(host)
        return ip
    except Exception:
        return None

class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # suppress logs

    def _json(self, code, data):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def do_GET(self):
        if self.path == "/health":
            self._json(200, {
                "status": "ok",
                "service": "was",
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "hostname": socket.gethostname(),
            })
        elif self.path == "/db-check":
            db_ip = resolve_host(DB_HOST)
            ok = check_redis(DB_HOST, DB_PORT)
            self._json(200, {
                "db_host": DB_HOST,
                "db_port": DB_PORT,
                "db_ip": db_ip,
                "db_reachable": ok,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            })
        elif self.path == "/info":
            self._json(200, {
                "service": "was",
                "hostname": socket.gethostname(),
                "db_host": DB_HOST,
                "db_port": DB_PORT,
                "uptime": time.monotonic(),
            })
        else:
            self._json(200, {
                "service": "was",
                "endpoints": ["/health", "/db-check", "/info"],
            })

if __name__ == "__main__":
    server = http.server.HTTPServer(("0.0.0.0", WAS_PORT), Handler)
    print(f"WAS running on :{WAS_PORT} (db={DB_HOST}:{DB_PORT})")
    server.serve_forever()
