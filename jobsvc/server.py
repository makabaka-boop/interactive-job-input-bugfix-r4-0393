"""Unix 域套接字服务端：行分隔 JSON 协议，仅本机访问。

协议（每行一个 JSON 对象，请求/响应一一对应）：

    {"op": "submit", "command": str, "args": [str...],
     "timeout"?: float, "output_max_bytes"?: int}
        -> {"ok": true, "job": {...snapshot...}}
    {"op": "status", "id": str}            -> {"ok": true, "job": {...}}
    {"op": "list"}                         -> {"ok": true, "jobs": [...]}
    {"op": "wait", "id": str, "timeout"?: number}
        -> {"ok": true, "job": {...}}
    {"op": "cancel", "id": str, "wait"?: bool, "timeout"?: number}
        -> {"ok": true, "job": {...}}
    {"op": "read", "id": str, "stream": "stdout"|"stderr",
     "offset": int, "max_bytes"?: int}
        -> {"ok": true, "data": <base64>, ...游标字段...}
    {"op": "shutdown"}

输出以 base64 传输，天然支持二进制内容与含换行的文本。
"""

from __future__ import annotations

import base64
import json
import logging
import os
import socket
import threading

from .service import JobService
from .states import JobError

log = logging.getLogger("jobsvc.server")

_MAX_LINE = 1 << 20


class UnixServer:
    def __init__(self, path: str, service: JobService | None = None,
                 accept_timeout: float = 1.0, on_shutdown=None):
        self.path = path
        self.service = service or JobService()
        self._owns_service = service is None
        self._on_shutdown = on_shutdown
        self._sock: socket.socket | None = None
        self._stop = threading.Event()
        self._accept_thread: threading.Thread | None = None
        self._workers: list[threading.Thread] = []
        self._workers_lock = threading.Lock()
        self._accept_timeout = accept_timeout

    def start(self) -> "UnixServer":
        if self._sock is not None:
            return self
        self.service.start()
        if os.path.exists(self.path):
            os.unlink(self.path)
        os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.bind(self.path)
        os.chmod(self.path, 0o600)
        sock.listen(64)
        sock.settimeout(self._accept_timeout)
        self._sock = sock
        self._accept_thread = threading.Thread(
            target=self._accept_loop, name="jobsvc-accept", daemon=True)
        self._accept_thread.start()
        return self

    def wait_closed(self, timeout: float = 10.0) -> None:
        if self._accept_thread is not None:
            self._accept_thread.join(timeout)
        with self._workers_lock:
            workers = list(self._workers)
        for t in workers:
            t.join(timeout)

    def close(self) -> None:
        """停止接收连接，关闭服务（杀死全部作业，唤醒阻塞的 wait），
        然后回收工作线程。顺序很重要：必须先关服务，否则卡在
        长时间 wait 请求上的工作线程永远不会返回。"""
        self._stop.set()
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None
        if self._accept_thread is not None:
            self._accept_thread.join(timeout=2.0)
        self.service.close()
        # service.close 已让所有作业终态化，阻塞在 wait 上的线程随即返回。
        with self._workers_lock:
            workers = list(self._workers)
        for t in workers:
            t.join(timeout=5.0)
        self._unlink_path()

    def _unlink_path(self) -> None:
        if os.path.exists(self.path):
            try:
                os.unlink(self.path)
            except OSError:
                pass

    # ---- 内部 -----------------------------------------------------------

    def _accept_loop(self) -> None:
        assert self._sock is not None
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            t = threading.Thread(
                target=self._serve_conn, args=(conn,), daemon=True)
            with self._workers_lock:
                self._workers.append(t)
            t.start()

    def _serve_conn(self, conn: socket.socket) -> None:
        conn.settimeout(None)
        f = conn.makefile("rwb", buffering=0)
        try:
            while True:
                line = f.readline(_MAX_LINE + 1)
                if not line:
                    return
                if len(line) > _MAX_LINE:
                    self._send(f, {"ok": False, "error": "请求行过大",
                                   "type": "JobError"})
                    return
                try:
                    req = json.loads(line.decode("utf-8"))
                except (ValueError, UnicodeDecodeError) as exc:
                    self._send(f, {"ok": False, "error": str(exc),
                                   "type": "JobError"})
                    continue
                if req.get("op") == "shutdown":
                    self._send(f, {"ok": True})
                    threading.Thread(target=self._shutdown_from_rpc,
                                     daemon=True).start()
                    return
                try:
                    resp = self._dispatch(req)
                except JobError as exc:
                    resp = {"ok": False, "error": str(exc),
                            "type": type(exc).__name__}
                except Exception as exc:  # 单个请求异常不能拖垮服务
                    log.exception("处理请求失败")
                    resp = {"ok": False, "error": str(exc),
                            "type": type(exc).__name__}
                self._send(f, resp)
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            try:
                f.close()
            except OSError:
                pass
            conn.close()

    def _shutdown_from_rpc(self) -> None:
        # 由 RPC 触发的关闭：先完成服务收尾，再通知宿主（如 CLI 主循环）退出。
        self._do_shutdown()
        if self._on_shutdown is not None:
            try:
                self._on_shutdown()
            except Exception:
                log.exception("on_shutdown 回调失败")

    def _do_shutdown(self) -> None:
        # 响应已发出，执行与 close() 相同的关闭顺序（在独立线程中进行，
        # 不能由正在服务该连接的工作线程等待自己结束）。
        self._stop.set()
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None
        if self._accept_thread is not None:
            self._accept_thread.join(timeout=2.0)
        self.service.close()
        with self._workers_lock:
            workers = [t for t in self._workers
                       if t is not threading.current_thread()]
        for t in workers:
            t.join(timeout=5.0)
        self._unlink_path()

    def _dispatch(self, req: dict) -> dict:
        svc = self.service
        op = req.get("op")
        if op == "submit":
            job = svc.submit(
                command=req["command"],
                args=list(req.get("args") or []),
                timeout=req.get("timeout"),
                output_max_bytes=req.get("output_max_bytes"),
            )
            return {"ok": True, "job": job.snapshot()}
        if op == "input":
            try:data=base64.b64decode(req["data"],validate=True)
            except (ValueError,TypeError) as exc:raise JobError('Invalid base64') from exc
            return {"ok":True,"receipt":svc.write_input(req["id"],req["seq"],data)}
        if op == "close_input":
            return {"ok":True,"input":svc.close_input(req["id"])}
        if op == "input_receipts":
            return {"ok":True,"receipts":svc.input_receipts(req["id"])}
        if op == "status":
            return {"ok": True, "job": svc.snapshot(req["id"])}
        if op == "list":
            return {"ok": True, "jobs": [j.snapshot() for j in svc.list_jobs()]}
        if op == "wait":
            job = svc.wait_for(req["id"], timeout=req.get("timeout"))
            return {"ok": True, "job": job.snapshot()}
        if op == "cancel":
            snap = svc.cancel(
                req["id"],
                wait=bool(req.get("wait", True)),
                timeout=req.get("timeout", 30.0),
            )
            return {"ok": True, "job": snap}
        if op == "read":
            r = svc.read_output(
                req["id"], req["stream"],
                offset=int(req.get("offset", 0)),
                max_bytes=int(req.get("max_bytes", 1 << 16)),
            )
            return {
                "ok": True,
                "stream": r["stream"],
                "data": base64.b64encode(r["data"]).decode("ascii"),
                "offset": r["offset"],
                "next_offset": r["next_offset"],
                "total": r["total"],
                "produced": r["produced"],
                "eof": r["eof"],
                "state": r["state"],
            }
        raise JobError(f"未知操作: {op!r}")

    @staticmethod
    def _send(f, obj: dict) -> None:
        f.write(json.dumps(obj, ensure_ascii=False).encode("utf-8"))
        f.write(b"\n")
