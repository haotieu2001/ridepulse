"""The Databricks REST API as a service principal (OAuth M2M), for the Lambdas: the window (#25) and the forwarder
(#26). Plain urllib, so a Lambda zip needs no dependencies. Credentials come from SSM (ADR-0002).
"""
from __future__ import annotations

import base64
import json
import urllib.error
import urllib.parse
import urllib.request
from typing import BinaryIO


class Databricks:
    def __init__(self, host: str, client_id: str, client_secret: str, timeout: float = 10):
        self.host, self.timeout = host.rstrip("/"), timeout
        self._auth = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
        self._token: str | None = None

    @classmethod
    def from_ssm(cls, ssm, prefix: str, principal: str, timeout: float = 10) -> Databricks:
        """`principal` names the SSM pair: <prefix>/<principal>_client_id and _client_secret, and <prefix>/host."""
        def get(name: str) -> str:
            return ssm.get_parameter(Name=f"{prefix}/{name}", WithDecryption=True)["Parameter"]["Value"]
        return cls(get("host"), get(f"{principal}_client_id"), get(f"{principal}_client_secret"), timeout)

    def _send(self, req: urllib.request.Request) -> bytes:
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:   # say what Databricks said, not just the status
            raise RuntimeError(f"Databricks {req.get_method()} {req.selector}: {e.code} "
                               f"{e.read().decode(errors='replace')[:500]}") from e

    def token(self) -> str:
        if self._token is None:
            req = urllib.request.Request(f"{self.host}/oidc/v1/token", method="POST",
                                         data=urllib.parse.urlencode({"grant_type": "client_credentials", "scope": "all-apis"}).encode())
            req.add_header("Authorization", f"Basic {self._auth}")
            req.add_header("Content-Type", "application/x-www-form-urlencoded")
            self._token = json.loads(self._send(req))["access_token"]
        return self._token

    def call(self, method: str, path: str, body: dict | None = None) -> dict:
        req = urllib.request.Request(f"{self.host}{path}", method=method,
                                     data=json.dumps(body).encode() if body is not None else None)
        req.add_header("Authorization", f"Bearer {self.token()}")
        req.add_header("Content-Type", "application/json")
        raw = self._send(req)
        return json.loads(raw) if raw else {}

    def upload(self, volume_path: str, stream: BinaryIO, length: int) -> None:
        """PUT a file into a UC Volume (Files API), streamed: the body is read in blocks, never held whole."""
        quoted = urllib.parse.quote(volume_path)
        req = urllib.request.Request(f"{self.host}/api/2.0/fs/files{quoted}?overwrite=true", method="PUT", data=stream)
        req.add_header("Authorization", f"Bearer {self.token()}")
        req.add_header("Content-Type", "application/octet-stream")
        req.add_header("Content-Length", str(length))
        self._send(req)
