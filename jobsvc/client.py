"""Unix 域套接字客户端：同步 RPC 封装 + 游标式输出迭代。"""

from __future__ import annotations

import base64
import json
import socket
from typing import Iterator


class ServiceError(Exception):
    """服务端返回的错误。"""

    def __init__(self, message: str, type_name: str = "JobError"):
        super().__init__(message)
        self.type_name = type_name


class JobClient:
    def __init__(self, path: str, timeout: float = 30.0):
        self.path = path
        self.timeout = timeout
        self._sock: socket.socket | None = None
        self._rf = None

    def connect(self) -> "JobClient":
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._sock.settimeout(self.timeout)
        self._sock.connect(self.path)
        self._rf = self._sock.makefile("rwb", buffering=0)
        return self

    def close(self) -> None:
        if self._rf is not None:
            try:
                self._rf.close()
            except OSError:
                pass
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
        self._sock = None
        self._rf = None

    def __enter__(self) -> "JobClient":
        return self.connect()

    def __exit__(self, *exc) -> None:
        self.close()

    def _call(self, req: dict) -> dict:
        assert self._rf is not None
        self._rf.write(json.dumps(req).encode("utf-8"))
        self._rf.write(b"\n")
        line = self._rf.readline()
        if not line:
            raise ConnectionError("服务端关闭了连接")
        resp = json.loads(line.decode("utf-8"))
        if not resp.get("ok"):
            raise ServiceError(resp.get("error", "未知错误"),
                               resp.get("type", "JobError"))
        return resp

    # ---- RPC ------------------------------------------------------------

    def submit(self, command: str, args=(), timeout=None,
               output_max_bytes=None) -> dict:
        return self._call({
            "op": "submit", "command": command, "args": list(args),
            **({"timeout": timeout} if timeout is not None else {}),
            **({"output_max_bytes": output_max_bytes}
               if output_max_bytes is not None else {}),
        })["job"]

    def write_input(self,job_id,seq,data):
        return self._call({"op":"input","id":job_id,"seq":seq,"data":base64.b64encode(data).decode("ascii")})["receipt"]

    def close_input(self,job_id):
        return self._call({"op":"close_input","id":job_id})["input"]

    def input_receipts(self,job_id):
        return self._call({"op":"input_receipts","id":job_id})["receipts"]

    def status(self, job_id: str) -> dict:
        return self._call({"op": "status", "id": job_id})["job"]

    def list_jobs(self) -> list[dict]:
        return self._call({"op": "list"})["jobs"]

    def wait(self, job_id: str, timeout: float | None = None) -> dict:
        return self._call(
            {"op": "wait", "id": job_id, "timeout": timeout})["job"]

    def cancel(self, job_id: str, wait: bool = True,
               timeout: float = 30.0) -> dict:
        return self._call({"op": "cancel", "id": job_id, "wait": wait,
                           "timeout": timeout})["job"]

    def read(self, job_id: str, stream: str, offset: int = 0,
             max_bytes: int = 1 << 16) -> dict:
        resp = self._call({"op": "read", "id": job_id, "stream": stream,
                           "offset": offset, "max_bytes": max_bytes})
        resp["data"] = base64.b64decode(resp["data"])
        return resp

    def shutdown(self) -> None:
        try:
            self._call({"op": "shutdown"})
        except (ConnectionError, OSError):
            pass

    # ---- 游标读取辅助 ---------------------------------------------------

    def iter_output(self, job_id: str, stream: str,
                    chunk: int = 1 << 16,
                    poll_interval: float = 0.05,
                    wait_terminal: bool = True) -> Iterator[bytes]:
        """从游标 0 开始流式拉取一路输出，直到该路 EOF（终态）。"""
        offset = 0
        while True:
            r = self.read(job_id, stream, offset, chunk)
            if r["data"]:
                yield r["data"]
            offset = r["next_offset"]
            if r["eof"]:
                return
            if not wait_terminal and r["state"] in (
                    "queued", "running"):
                # 调用方只想读当前已保存内容。
                if offset == r["total"]:
                    return
            import time
            time.sleep(poll_interval)
