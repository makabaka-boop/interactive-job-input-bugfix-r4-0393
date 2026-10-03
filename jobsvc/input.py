"""交互输入通道：按序号可靠地把客户端片段写入作业 stdin。

约定（见 README“交互输入”）：

* 片段从序号 0 开始连续接受；同序号同内容重试是幂等重发，返回同一张
  回执且不会再次写入；同序号异内容一律拒绝（歧义重试）。
* 每个片段在被完整写入管道前保持 pending/delivering；``written`` 是
  实际写进管道的字节数，只有写满 ``size`` 才记 delivered。
* 结束输入（``request_close``）后不再接受新片段，但 EOF 必须等到所有
  已接受片段都写完才发送，绝不截断。
* 取消 / 超时 / 超限 / 进程退出 / 写端破裂（``finish``）后，尚未送达
  的片段得到 status="failed" 与明确的 reason；delivered 的结果不变。
* 单片上限 ``MAX_FRAGMENT``（64KiB）；待处理（未写满）合计上限
  ``MAX_PENDING``（1MiB），做背压防止客户端把内存撑爆。

所有方法都用一把锁保护：监控线程在 poll 循环里 drain，RPC 工作线程
可以同时 admit / 查回执。
"""

from __future__ import annotations

import collections
import hashlib
import os
import threading

from .states import InvalidArguments

# 单个片段的最大字节数。
MAX_FRAGMENT = 64 * 1024
# 所有“未写满”片段合计字节数上限（背压窗口）。
MAX_PENDING = 1 << 20

# drain 返回值：EOF 哨兵已发送 / 写端破裂；None 表示本轮无终局动作。
EOF_SENT = "eof"
BROKEN_PIPE = "broken_pipe"

# 未能送达时的回执原因。
REASON_CANCELLED = "cancelled"
REASON_TIMED_OUT = "timed_out"
REASON_OUTPUT_LIMIT = "output_limit"
REASON_EXITED = "exited"
REASON_BROKEN_PIPE = "broken_pipe"
REASON_SPAWN_ERROR = "spawn_error"

_RECEIPT_FIELDS = ("seq", "size", "written", "status", "reason")


class InputChannel:
    def __init__(self):
        self._items: dict[int, dict] = {}
        # 等待发送的片段序号；队头之外的项 status 保持 pending。
        self._queue: collections.deque[int] = collections.deque()
        # 正在分次写入（可能部分写入）的片段序号。
        self._inflight: int | None = None
        self._next_seq = 0
        self.closing = False       # 已请求 EOF，但队列可能还没排空
        self.closed = False        # 终局：EOF 已发送或已失败
        self._close_reason: str | None = None
        self._lock = threading.Lock()
        # closed 翻转时唤醒等待者（wait_closed）。
        self._closed_event = threading.Event()

    # ---- 接受片段（RPC 线程） -------------------------------------------

    def admit(self, seq, data):
        """接受一个片段，返回该序号的回执（投影，不含数据）。

        同序号同内容重试：返回既有回执（无论它现在是何种状态），绝不
        重新入队；同序号异内容：抛 InvalidArguments。
        """
        # bool 是 int 的子类，必须显式排除。
        if not isinstance(seq, int) or isinstance(seq, bool) or seq < 0:
            raise InvalidArguments("seq 必须是非负整数")
        if not isinstance(data, bytes):
            raise InvalidArguments("data 必须是 bytes")
        digest = hashlib.sha256(data).digest()
        with self._lock:
            existing = self._items.get(seq)
            if existing is not None:
                # 幂等重试优先于 closed 判定：即便通道已终局，重放同一
                # 内容也只会拿到同一张回执，不会触发任何新写入。
                if existing["digest"] != digest:
                    raise InvalidArguments(
                        f"序号 {seq} 已接受过不同内容，异内容重试被拒绝")
                return self._receipt_locked(existing)
            if self.closed:
                hint = f"（{self._close_reason}）" if self._close_reason else ""
                raise InvalidArguments(f"输入已关闭{hint}，不再接受新片段")
            if self.closing:
                raise InvalidArguments("输入正在结束，不再接受新片段")
            if seq != self._next_seq:
                raise InvalidArguments(
                    f"片段序号必须从 0 开始连续，下一个应为 "
                    f"{self._next_seq}，收到 {seq}")
            size = len(data)
            if size > MAX_FRAGMENT:
                raise InvalidArguments(
                    f"单个片段不能超过 {MAX_FRAGMENT} 字节，收到 {size}")
            pending = self._pending_bytes_locked()
            if pending + size > MAX_PENDING:
                raise InvalidArguments(
                    f"待处理输入合计不能超过 {MAX_PENDING} 字节"
                    f"（当前待处理 {pending}）")
            item = {
                "seq": seq,
                "size": size,
                "written": 0,
                "status": "pending",
                "reason": None,
                "data": data,
                "digest": digest,
            }
            self._items[seq] = item
            self._queue.append(seq)
            self._next_seq = seq + 1
            return self._receipt_locked(item)

    def request_close(self):
        """请求结束输入：停止接受新片段，已接受片段排空后发送 EOF。

        幂等；不阻塞，排空过程在监控线程的 drain 中完成。
        """
        with self._lock:
            if not self.closed:
                self.closing = True
            return {"closing": True, "closed": self.closed}

    def wait_closed(self, timeout: float | None = None) -> bool:
        """等待通道到达终局（EOF 已发送或所有片段已被最终裁决）。"""
        return self._closed_event.wait(timeout)

    # ---- 监控线程驱动 ---------------------------------------------------

    def has_work(self) -> bool:
        """是否需要把 stdin fd 挂进 poll（有待写片段或待发 EOF）。"""
        with self._lock:
            return not self.closed and (
                self._inflight is not None
                or bool(self._queue) or self.closing)

    def drain(self, fd, writer=None) -> str | None:
        """在 fd 可写（POLLOUT）时推进一步。

        非阻塞、部分写安全：每次只写“当前片段剩余部分”，写多少记多少；
        片段顺序与完整字节由此得到保证。

        返回：
            None          —— 仍有工作（或本次因管道满未写成），继续 poll
            EOF_SENT      —— 所有片段已写完且 EOF 哨兵已消费，调用方应
                             关闭 stdin fd
            BROKEN_PIPE   —— 写端破裂（读端关闭），未送达片段已失败，
                             调用方应关闭 stdin fd
        """
        write = writer or os.write
        with self._lock:
            if self.closed:
                return None

            if self._inflight is None:
                if self._queue:
                    self._inflight = self._queue.popleft()
                elif self.closing:
                    # 已接受片段全部送达，此刻才允许 EOF。
                    self.closed = True
                    self._close_reason = "eof"
                    self._closed_event.set()
                    return EOF_SENT
                else:
                    return None

            item = self._items[self._inflight]
            remaining = memoryview(item["data"])[item["written"]:]
            try:
                n = write(fd, remaining)
            except BlockingIOError:
                return None  # 管道暂满，下一个 POLLOUT 继续，状态不变
            except InterruptedError:
                return None  # 被信号打断，重试
            except OSError:
                # EPIPE / ECONNRESET 等：读端已经没了，队列中（含当前
                # 只写了一部分的）片段全部不可能再送达。
                self._fail_all_locked(REASON_BROKEN_PIPE)
                self._closed_event.set()
                return BROKEN_PIPE

            if n > 0:
                item["written"] += n
            if item["written"] == item["size"]:
                item["status"] = "delivered"
                item["reason"] = None
                item["data"] = b""  # 已送达，释放负载（digest 留作幂等校验）
                self._inflight = None
            else:
                item["status"] = "delivering"
            return None

    def finish(self, reason: str) -> None:
        """终局收尾：未送达片段全部失败，关闭后拒绝一切新增。

        幂等：先发生的裁决（取消/退出/破裂/EOF）永远不被覆盖。
        """
        with self._lock:
            if self.closed:
                return
            self._fail_all_locked(reason)
            self._closed_event.set()

    # ---- 回执查询 -------------------------------------------------------

    def receipts(self) -> list[dict]:
        with self._lock:
            return [
                self._receipt_locked(self._items[seq])
                for seq in sorted(self._items)
            ]

    # ---- 内部（持锁调用） -----------------------------------------------

    def _pending_bytes_locked(self) -> int:
        return sum(
            it["size"] - it["written"]
            for it in self._items.values()
            if it["status"] in ("pending", "delivering"))

    def _fail_all_locked(self, reason: str) -> None:
        self.closed = True
        self.closing = False
        self._close_reason = reason
        self._queue.clear()
        self._inflight = None
        for item in self._items.values():
            if item["status"] in ("pending", "delivering"):
                # written 保留实际写进管道的字节数，失败结果也要诚实。
                item["status"] = "failed"
                item["reason"] = reason
                item["data"] = b""

    @staticmethod
    def _receipt_locked(item: dict) -> dict:
        return {k: item[k] for k in _RECEIPT_FIELDS}
