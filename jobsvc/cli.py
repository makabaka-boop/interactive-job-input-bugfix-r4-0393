"""命令行入口。

    python -m jobsvc serve [--socket PATH]
    python -m jobsvc submit  COMMAND [ARG ...] [--timeout S] [--max-bytes N]
    python -m jobsvc status  JOB_ID
    python -m jobsvc wait    JOB_ID [--timeout S]
    python -m jobsvc cancel  JOB_ID
    python -m jobsvc read    JOB_ID stdout|stderr [--offset N]
    python -m jobsvc tail    JOB_ID stdout|stderr        # 流式跟到终态
    python -m jobsvc list
    python -m jobsvc commands
    python -m jobsvc shutdown
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

from . import registry
from .client import JobClient, ServiceError
from .server import UnixServer
from .service import JobService

DEFAULT_SOCKET = os.environ.get("JOBSVC_SOCKET", "/tmp/jobsvc.sock")


def _print(obj) -> None:
    print(json.dumps(obj, indent=2, ensure_ascii=False))


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="jobsvc")
    p.add_argument("--socket", default=DEFAULT_SOCKET)
    sub = p.add_subparsers(dest="op", required=True)

    s_serve = sub.add_parser("serve")
    s_serve.add_argument("--max-concurrent", type=int, default=2)
    s_serve.add_argument("--kill-grace", type=float, default=2.0)

    s_sub = sub.add_parser("submit")
    s_sub.add_argument("command")
    s_sub.add_argument("args", nargs="*")
    s_sub.add_argument("--timeout", type=float)
    s_sub.add_argument("--max-bytes", type=int)

    for name in ("status", "wait", "cancel"):
        sp = sub.add_parser(name)
        sp.add_argument("job_id")
        if name == "wait":
            sp.add_argument("--timeout", type=float)

    s_read = sub.add_parser("read")
    s_read.add_argument("job_id")
    s_read.add_argument("stream", choices=("stdout", "stderr"))
    s_read.add_argument("--offset", type=int, default=0)
    s_read.add_argument("--max-bytes", type=int, default=1 << 16)

    s_tail = sub.add_parser("tail")
    s_tail.add_argument("job_id")
    s_tail.add_argument("stream", choices=("stdout", "stderr"))

    sub.add_parser("list")
    sub.add_parser("commands")
    sub.add_parser("shutdown")

    ns = p.parse_args(argv)

    if ns.op == "serve":
        import threading
        svc = JobService(max_concurrent=ns.max_concurrent,
                         kill_grace=ns.kill_grace)
        stopped = threading.Event()
        server = UnixServer(ns.socket, svc, on_shutdown=stopped.set)
        server.start()
        sys.stderr.write(f"jobsvc 监听 {ns.socket}，最多 "
                         f"{ns.max_concurrent} 个并发作业\n")
        try:
            while not stopped.wait(timeout=1.0):
                pass
        except KeyboardInterrupt:
            sys.stderr.write("正在关闭...\n")
        finally:
            server.close()
        return 0

    if ns.op == "commands":
        for name in registry.command_names():
            e = registry.get(name)
            params = ", ".join(
                f"{p.name}[{p.lo}..{p.hi}]" for p in e.params) or "无参数"
            print(f"{name:20s} {params}")
        return 0

    with JobClient(ns.socket) as client:
        try:
            if ns.op == "submit":
                _print(client.submit(ns.command, ns.args,
                                     timeout=ns.timeout,
                                     output_max_bytes=ns.max_bytes))
            elif ns.op == "status":
                _print(client.status(ns.job_id))
            elif ns.op == "wait":
                _print(client.wait(ns.job_id, timeout=ns.timeout))
            elif ns.op == "cancel":
                _print(client.cancel(ns.job_id))
            elif ns.op == "read":
                r = client.read(ns.job_id, ns.stream, ns.offset,
                                ns.max_bytes)
                sys.stdout.buffer.write(r["data"])
                sys.stderr.write(
                    f"\n[offset {r['offset']} -> {r['next_offset']} "
                    f"eof={r['eof']} state={r['state']}]\n")
            elif ns.op == "tail":
                for chunk in client.iter_output(ns.job_id, ns.stream):
                    sys.stdout.buffer.write(chunk)
                    sys.stdout.buffer.flush()
            elif ns.op == "list":
                _print(client.list_jobs())
            elif ns.op == "shutdown":
                client.shutdown()
                print("已请求关闭")
        except ServiceError as exc:
            sys.stderr.write(f"错误({exc.type_name}): {exc}\n")
            return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
