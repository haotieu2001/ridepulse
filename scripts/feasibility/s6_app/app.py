"""S6 feasibility: the smallest Databricks App. Standard library only, no build step."""
import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps({
            "test": "S6",
            "status": "ok",
            "app": os.environ.get("DATABRICKS_APP_NAME"),
            "host": os.environ.get("DATABRICKS_HOST"),
            # Apps forward the signed-in user's identity in these headers.
            "user": self.headers.get("X-Forwarded-Email"),
        }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    port = int(os.environ.get("DATABRICKS_APP_PORT", "8000"))
    HTTPServer(("0.0.0.0", port), Handler).serve_forever()
