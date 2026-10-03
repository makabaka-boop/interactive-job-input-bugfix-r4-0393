"""InputChannel 单元测试。

覆盖：
* 序号从 0 连续、同序号同内容幂等重发（同一张回执、不重复写入）
* 同序号异内容重试拒绝、bool/负数/非 bytes 参数拒绝
* 单片 64KiB 与待处理合计 1MiB 上限；送达后释放待处理额度
* 分次部分写入时 written 字节与 delivering 状态，写满才 delivered
* 结束输入：已接受片段全部写完才发 EOF（绝不截断），之后拒绝新增
* 写端破裂（EPIPE）：在途与排队片段全部 failed/broken_pipe
* finish：取消/退出后未送达片段 failed 且原因明确；delivered 不被改写；
  finish 幂等（首个原因不被覆盖）；回执按序号排序
"""

from __future__ import annotations

import errno
import os
import unittest

from jobsvc.input import (
    BROKEN_PIPE,
    EOF_SENT,
    InputChannel,
    MAX_FRAGMENT,
    MAX_PENDING,
)
from jobsvc.states import InvalidArguments


class FakeWriter:
    """模拟管道写端：容量上限 + 可限制每次最多写 max_chunk 字节。"""

    def __init__(self, capacity=65536, max_chunk=None, broken=False):
        self.capacity = capacity
        self.used = 0
        self.max_chunk = max_chunk
        self.broken = broken
        self.written = bytearray()   # 在管内容（读端取走后会缩短）
        self.delivered = bytearray()  # 累计“写进管道”的全部字节

    def __call__(self, fd, data):
        if self.broken:
            raise OSError(errno.EPIPE, "Broken pipe")
        room = self.capacity - self.used
        if room <= 0:
            raise BlockingIOError(errno.EAGAIN, "Resource temporarily unavailable")
        n = min(len(data), room)
        if self.max_chunk is not None:
            n = min(n, self.max_chunk)
        self.written += bytes(data[:n])
        self.delivered += bytes(data[:n])
        self.used += n
        return n

    def free(self, n: int | None = None):
        """模拟读端取走字节。"""
        take = self.used if n is None else min(n, self.used)
        del self.written[:take]
        self.used -= take


def pump(ch, writer, until_eof=False, max_steps=100000):
    """模拟监控线程在“fd 可写”时反复 drain；writer 满时由读端排空。"""
    actions = []
    for _ in range(max_steps):
        action = ch.drain(-1, writer=writer)
        if action is not None:
            actions.append(action)
        if until_eof and EOF_SENT in actions:
            return actions
        if not until_eof and not ch.has_work():
            return actions
        # 子进程持续读取，腾出全部空间（除特别构造的反压场景外）。
        writer.free()
    raise AssertionError("pump 超过最大步数")


class AdmitTest(unittest.TestCase):
    def test_sequential_from_zero_and_receipt_shape(self):
        ch = InputChannel()
        r0 = ch.admit(0, b"abc")
        self.assertEqual(
            r0, {"seq": 0, "size": 3, "written": 0,
                 "status": "pending", "reason": None})
        r1 = ch.admit(1, b"de")
        self.assertEqual(r1["seq"], 1)
        # 回执按序号排序。
        self.assertEqual([r["seq"] for r in ch.receipts()], [0, 1])

    def test_idempotent_same_seq_same_content(self):
        ch = InputChannel()
        first = ch.admit(0, b"hello")
        again = ch.admit(0, b"hello")
        self.assertEqual(first, again)
        ch.request_close()
        writer = FakeWriter()
        pump(ch, writer, until_eof=True)
        # 重试绝不导致重复写入。
        self.assertEqual(bytes(writer.delivered), b"hello")

    def test_same_seq_different_content_rejected(self):
        ch = InputChannel()
        ch.admit(0, b"hello")
        with self.assertRaises(InvalidArguments):
            ch.admit(0, b"hellX")
        with self.assertRaises(InvalidArguments):
            ch.admit(0, b"helloo")  # 长度不同也算异内容
        # 被拒后队列与内容不变。
        self.assertEqual(ch.receipts()[0]["size"], 5)

    def test_gap_and_start_seq_rejected(self):
        ch = InputChannel()
        with self.assertRaises(InvalidArguments):
            ch.admit(1, b"x")
        ch.admit(0, b"x")
        with self.assertRaises(InvalidArguments):
            ch.admit(2, b"x")
        ch.admit(1, b"x")

    def test_bad_arguments(self):
        ch = InputChannel()
        for bad in (True, False, -1, -100, "0", 1.0, None):
            with self.assertRaises(InvalidArguments):
                ch.admit(bad, b"x")
        for bad in ("abc", bytearray(b"x"), None, 7):
            with self.assertRaises(InvalidArguments):
                ch.admit(0, bad)

    def test_fragment_size_limit(self):
        ch = InputChannel()
        ch.admit(0, b"x" * MAX_FRAGMENT)  # 恰好上限，接受
        with self.assertRaises(InvalidArguments):
            ch.admit(1, b"x" * (MAX_FRAGMENT + 1))

    def test_pending_total_limit_and_release(self):
        ch = InputChannel()
        frag = b"q" * MAX_FRAGMENT
        for i in range(MAX_PENDING // MAX_FRAGMENT):  # 恰好 1 MiB
            ch.admit(i, frag)
        with self.assertRaises(InvalidArguments):
            ch.admit(16, b"q")
        # 空管道中一次 drain 把队头 64KiB 全写出 -> 额度释放，可再收。
        writer = FakeWriter(capacity=1 << 20)
        self.assertIsNone(ch.drain(-1, writer=writer))
        self.assertEqual(ch.receipts()[0]["status"], "delivered")
        ch.admit(16, b"q")

    def test_reject_after_close_or_finish(self):
        ch = InputChannel()
        ch.admit(0, b"a")
        ch.request_close()
        with self.assertRaises(InvalidArguments):
            ch.admit(1, b"b")
        ch.finish("cancelled")
        with self.assertRaises(InvalidArguments):
            ch.admit(2, b"c")
        # 已存在序号的同内容重放仍然只返回既有回执，不报错、不重发。
        self.assertEqual(ch.admit(0, b"a")["status"], "failed")


class DeliveryTest(unittest.TestCase):
    def test_multiple_fragments_order_and_full_bytes(self):
        ch = InputChannel()
        payloads = [b"first-", b"second-", bytes(range(256)) * 10]
        for i, p in enumerate(payloads):
            ch.admit(i, p)
        ch.request_close()
        writer = FakeWriter(capacity=1 << 20)
        actions = pump(ch, writer, until_eof=True)
        self.assertIn(EOF_SENT, actions)
        self.assertEqual(bytes(writer.delivered), b"".join(payloads))
        for i, p in enumerate(payloads):
            r = ch.receipts()[i]
            self.assertEqual(r["status"], "delivered")
            self.assertEqual(r["written"], len(p))
        self.assertTrue(ch.closed)
        self.assertTrue(ch.wait_closed(timeout=0.1))

    def test_partial_writes_keep_order_and_byte_count(self):
        ch = InputChannel()
        ch.admit(0, b"A" * 100)
        ch.admit(1, b"B" * 50)
        ch.request_close()
        # 管道每次只收 7 字节：强制大量部分写。
        writer = FakeWriter(capacity=7, max_chunk=7)
        saw_delivering = False
        for _ in range(100000):
            action = ch.drain(-1, writer=writer)
            receipts = ch.receipts()
            if any(r["status"] == "delivering" for r in receipts):
                saw_delivering = True
            if action == EOF_SENT:
                break
            writer.free()
        else:
            self.fail("未到达 EOF")
        self.assertTrue(saw_delivering, "应当经历过 delivering 部分写状态")
        self.assertEqual(bytes(writer.delivered), b"A" * 100 + b"B" * 50)
        self.assertTrue(all(
            r["status"] == "delivered" for r in ch.receipts()))

    def test_full_pipe_blocks_without_losing_bytes(self):
        ch = InputChannel()
        ch.admit(0, b"zz")
        writer = FakeWriter(capacity=0)  # 管道已满
        self.assertIsNone(ch.drain(-1, writer=writer))
        r = ch.receipts()[0]
        self.assertEqual(r["written"], 0)
        self.assertEqual(r["status"], "pending")  # 不能谎称送达
        # 读端取走后继续写，最终写满。
        writer.capacity = 2
        self.assertIsNone(ch.drain(-1, writer=writer))
        self.assertEqual(ch.receipts()[0]["status"], "delivered")
        self.assertEqual(bytes(writer.delivered), b"zz")

    def test_close_waits_for_accepted_fragments_before_eof(self):
        ch = InputChannel()
        ch.admit(0, b"abc")
        ch.admit(1, b"def")
        self.assertEqual(ch.request_close(),
                         {"closing": True, "closed": False})
        writer = FakeWriter()
        self.assertTrue(ch.has_work())
        self.assertIsNone(ch.drain(-1, writer=writer))   # seq0
        self.assertFalse(ch.closed)                       # 还没 EOF
        self.assertIsNone(ch.drain(-1, writer=writer))   # seq1
        self.assertFalse(ch.closed)
        # 队列排空后的下一次 drain 才发 EOF 哨兵。
        self.assertEqual(ch.drain(-1, writer=writer), EOF_SENT)
        self.assertTrue(ch.closed)
        self.assertEqual(bytes(writer.delivered), b"abcdef")
        # EOF 之后 drain 是无害空操作。
        self.assertIsNone(ch.drain(-1, writer=writer))


class FailureTest(unittest.TestCase):
    def test_broken_pipe_fails_inflight_and_queued(self):
        ch = InputChannel()
        ch.admit(0, b"aaaa")
        ch.admit(1, b"bbbb")
        ch.admit(2, b"cccc")
        action = ch.drain(-1, writer=FakeWriter(broken=True))
        self.assertEqual(action, BROKEN_PIPE)
        self.assertTrue(ch.closed)
        for r in ch.receipts():
            self.assertEqual(r["status"], "failed")
            self.assertEqual(r["reason"], "broken_pipe")
        # 终局后继续 drain 不重复抛错。
        self.assertIsNone(ch.drain(-1, writer=FakeWriter(broken=True)))

    def test_finish_marks_undelivered_failed_and_keeps_reason(self):
        ch = InputChannel()
        ch.admit(0, b"aaaa")
        ch.admit(1, b"bbbb")
        writer = FakeWriter()
        self.assertIsNone(ch.drain(-1, writer=writer))  # seq0 送达
        ch.finish("cancelled")
        r0, r1 = ch.receipts()
        self.assertEqual(r0["status"], "delivered")
        self.assertIsNone(r0["reason"])
        self.assertEqual(r1["status"], "failed")
        self.assertEqual(r1["reason"], "cancelled")
        self.assertEqual(r1["written"], 0)
        # finish 幂等：后到的原因不能覆盖先发生的裁决。
        ch.finish("exited")
        self.assertEqual(ch.receipts()[1]["reason"], "cancelled")

    def test_partial_then_finish_retains_actual_written_bytes(self):
        ch = InputChannel()
        ch.admit(0, b"A" * 100)
        # 每次只写 30 字节的“慢管道”。
        writer = FakeWriter(capacity=1 << 20, max_chunk=30)
        ch.drain(-1, writer=writer)
        self.assertEqual(ch.receipts()[0]["status"], "delivering")
        ch.finish("timed_out")
        r = ch.receipts()[0]
        self.assertEqual(r["status"], "failed")
        self.assertEqual(r["reason"], "timed_out")
        self.assertEqual(r["written"], 30)  # 实际写进管道的字节如实保留

    def test_real_pipe_e2e_order_and_eof(self):
        """真实管道：监控线程只在可写时 drain，内容与顺序、EOF 均正确。"""
        ch = InputChannel()
        payloads = [b"hello\n", bytes(range(256)), b"tail" * 10000]
        for i, p in enumerate(payloads):
            ch.admit(i, p)
        ch.request_close()
        r, w = os.pipe()
        os.set_blocking(w, False)
        got = bytearray()
        import select
        poll = select.poll()
        poll.register(w, select.POLLOUT)
        poll_r = select.poll()
        poll_r.register(r, select.POLLIN)
        for _ in range(100000):
            if poll.poll(0.01):
                action = ch.drain(w)
                if action == EOF_SENT:
                    break
            if poll_r.poll(0):
                got += os.read(r, 1 << 20)
        else:
            self.fail("未到 EOF")
        os.close(w)
        while True:
            chunk = os.read(r, 1 << 20)
            if not chunk:
                break
            got += chunk
        os.close(r)
        self.assertEqual(bytes(got), b"".join(payloads))
        self.assertTrue(all(
            x["status"] == "delivered" for x in ch.receipts()))


if __name__ == "__main__":
    unittest.main()
