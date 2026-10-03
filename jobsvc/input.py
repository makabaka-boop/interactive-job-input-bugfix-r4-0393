"""交互输入通道：按序号输送片段，提供可查询的送达回执。

客户端把输入切成片段，用从 0 开始、必须连续的序号 ``seq`` 提交：

* 同 ``seq`` 同内容重试是**幂等**的——返回同一个片段回执，片段绝不重复
  写入；同 ``seq`` **异内容**重试直接拒绝（``InvalidArguments``）。
* 序号必须连续（下一个 seq 恰为已接受最大 seq + 1），乱序拒绝，保证
  子进程收到的字节顺序与片段顺序一致。
* 单片上限 :data:`MAX_CHUNK`（64KiB）；尚未写完的片段合计上限
  :data:`MAX_PENDING`（1MiB），超过则拒绝新片段，由调用方稍后重试
  （已接受的同序号片段仍可幂等查询）。

片段状态（``receipts()`` / ``admit()`` 回执均含
``seq/size/written/status/reason``）：

``pending``
    已接受，等待或正在写入子进程管道；``written`` 是已经被管道实际
    接受的字节数（可能是部分写入）。
``delivered``
    全部 ``size`` 字节都已实际写入管道（``written == size``）。
``failed``
    未能送达：作业被取消 / 超时 / 超限杀死、进程已退出或管道断裂；
    ``reason`` 给出明确原因，``written`` 是此前已写出的字节数。

请求结束输入（``request_close``）后不再接受新片段，但已接受片段会
继续按序写完，**最后**才关闭管道向子进程发送 EOF——不会截断在途数据。
"""

from __future__ import annotations

import errno
import os
import threading
from collections import deque

from .states import InvalidArguments

# 单片上限 64KiB。
MAX_CHUNK = 1 << 16
# 尚未写完的片段合计上限 1MiB。
MAX_PENDING = 1 << 20
# 单次 os.write 最多尝试字节数（管道为非阻塞，实际返回可能更短）。
_WRITE_CHUNK = 1 << 16


class InputChannel:
    """单个作业的 stdin 输送状态机。可跨线程使用（RPC 线程与监控线程）。"""

    def __init__(self):
        self._lock = threading.Lock()
        # seq -> 片段记录（含 data），终态后仍保留以供回执查询。
        self.items: dict[int, dict] = {}
        # 尚未写完的片段 seq，按提交顺序排列。
        self.queue: deque[int] = deque()
        self.closing = False   # 已请求结束：拒绝新片段，排空后发 EOF
        self.closed = False   # 已终结：EOF 已发，或被 finish/write 错误终结

    # ---- 接收片段（RPC 线程） -------------------------------------------

    def admit(self, seq, data):
        """接受一个片段；返回该片段的回执（不含 data 本体）。

        同 seq 同内容重试返回原回执（幂等，不重复入队）；同 seq 异内容、
        序号不连续、超限或通道已关闭均抛 InvalidArguments。
        """
        if isinstance(seq, bool) or not isinstance(seq, int):
            raise InvalidArguments("seq 必须是非负整数")
        if not isinstance(data, bytes):
            raise InvalidArguments("data 必须是 bytes（base64 解码后）")
        if seq < 0:
            raise InvalidArguments("seq 必须 >= 0")
        if len(data) > MAX_CHUNK:
            raise InvalidArguments(
                f"单个输入片段不能超过 {MAX_CHUNK} 字节（64KiB），"
                f"收到 {len(data)} 字节")

        with self._lock:
            if self.closed or self.closing:
                raise InvalidArguments("输入通道已关闭，不再接受片段")

            existing = self.items.get(seq)
            if existing is not None:
                if existing["data"] != data:
                    raise InvalidArguments(
                        f"序号 {seq} 已用于不同内容的片段；"
                        "同序号重试必须发送完全相同的字节")
                # 幂等重试：不重新入队、不重复写入，返回同一回执。
                return self._receipt(existing)

            expected = (max(self.items) + 1) if self.items else 0
            if seq != expected:
                raise InvalidArguments(
                    f"片段序号必须从 0 开始连续提交：下一个应为 "
                    f"{expected}，收到 {seq}")

            outstanding = sum(
                it["size"] - it["written"]
                for it in self.items.values()
                if it["status"] == "pending")
            if outstanding + len(data) > MAX_PENDING:
                raise InvalidArguments(
                    f"待写入输入合计不能超过 {MAX_PENDING} 字节（1MiB）；"
                    "请在部分片段送达后重试")

            item = {
                "seq": seq,
                "size": len(data),
                "written": 0,
                "status": "pending",
                "reason": None,
                "data": data,
            }
            self.items[seq] = item
            self.queue.append(seq)
            return self._receipt(item)

    def request_close(self):
        """请求结束输入：拒绝新片段，已接受片段排空后发送 EOF。幂等。"""
        with self._lock:
            self.closing = True
            return {"closing": True, "closed": self.closed}

    # ---- 输送（监控线程，fd 为非阻塞管道写端） ---------------------------

    def wants_write(self) -> bool:
        """是否有片段等待写入（决定是否订阅 POLLOUT）。"""
        with self._lock:
            return not self.closed and bool(self.queue)

    def should_eof_now(self) -> bool:
        """已请求结束且已接受片段全部写完时返回 True（仅一次）。

        返回 True 时通道已切换到终结态，调用方应立即关闭管道写端。
        """
        with self._lock:
            if not self.closed and self.closing and not self.queue:
                self.closed = True
                return True
            return False

    def write_ready(self, fd, writer=None) -> str:
        """fd 可写时推进队头片段，尽可能多写。

        返回：
            "progress" —— 有字节写出，可能仍有剩余；
            "retry"    —— 管道暂不可写（EAGAIN/零写），等下一次 POLLOUT；
            "eof"      —— 最后片段写完且已请求结束，调用方应关闭 fd；
            "broken"   —— 管道断裂/写出错，未送达片段已置 failed 回执，
                          调用方应关闭 fd。
        """
        do_write = writer or os.write
        with self._lock:
            if self.closed:
                return "broken"

            # 允许空片段：无字节可写，立即视为送达并推进到下一片。
            while self.queue and self.items[self.queue[0]]["size"] == 0:
                empty = self.items[self.queue[0]]
                empty["status"] = "delivered"
                self.queue.popleft()

            if not self.queue:
                if self.closing:
                    self.closed = True
                    return "eof"
                return "retry"

            item = self.items[self.queue[0]]
            start = item["written"]
            payload = item["data"][start:start + _WRITE_CHUNK]
            try:
                n = do_write(fd, payload)
            except (BlockingIOError, InterruptedError):
                return "retry"
            except OSError as exc:
                reason = ("broken_pipe" if exc.errno == errno.EPIPE
                          else "write_error")
                self._finish_locked(reason)
                return "broken"

            if not isinstance(n, int) or n <= 0:
                # 非阻塞写返回 0 不应发生；按“暂不可写”处理，避免空转外
                # 也不虚构送达字节。
                return "retry"

            item["written"] += n
            if item["written"] >= item["size"]:
                item["status"] = "delivered"
                self.queue.popleft()

            if not self.queue and self.closing:
                # 已接受片段全部写完：此刻才允许 EOF。
                self.closed = True
                return "eof"
            return "progress"

    # ---- 终结（取消 / 退出 / 超时 / 管道断裂） ---------------------------

    def finish(self, reason: str) -> None:
        """通道因作业取消、退出、超时等原因终结。

        尚未完全送达的片段置 failed 并记录原因与已写字节；已 delivered
        的回执保持成功。幂等：先发生的终结原因不被覆盖。
        """
        with self._lock:
            if self.closed:
                return
            self._finish_locked(reason)

    def _finish_locked(self, reason: str) -> None:
        # 调用方持 self._lock
        for item in self.items.values():
            if item["status"] == "pending":
                item["status"] = "failed"
                item["reason"] = reason
        self.queue.clear()
        self.closing = True
        self.closed = True

    # ---- 回执查询 --------------------------------------------------------

    def receipts(self) -> list[dict]:
        """按 seq 顺序返回全部片段回执。"""
        with self._lock:
            return [
                self._receipt(self.items[seq])
                for seq in sorted(self.items)
            ]

    @staticmethod
    def _receipt(item: dict) -> dict:
        return {
            "seq": item["seq"],
            "size": item["size"],
            "written": item["written"],
            "status": item["status"],
            "reason": item["reason"],
        }
