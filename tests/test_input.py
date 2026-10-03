"""InputChannel 单元测试：幂等重试、顺序、完整字节、关闭与失败回执。"""

from __future__ import annotations

import os
import unittest

from jobsvc.input import MAX_CHUNK, MAX_PENDING, InputChannel
from jobsvc.states import InvalidArguments


def _flush(ch: InputChannel, pipe_fd: int):
    """把通道全部待写内容排空到可写管道（reader 在持续读取）。"""
    actions = []
    while True:
        a = ch.write_ready(pipe_fd)
        actions.append(a)
        if a in ("eof", "broken"):
            return actions
        if a == "retry":
            raise AssertionError("可写管道不应返回 retry")
        # progress：继续写


class InputChannelTest(unittest.TestCase):
    def _pipe(self):
        r, w = os.pipe()

        def safe_close(fd):
            try:
                os.close(fd)
            except OSError:
                pass

        self.addCleanup(safe_close, r)
        self.addCleanup(safe_close, w)
        return r, w

    # ---- 参数校验 -------------------------------------------------------

    def test_rejects_bad_seq_and_data_types(self):
        ch = InputChannel()
        for bad_seq in ("0", 1.0, True, -1, None):
            with self.assertRaises(InvalidArguments):
                ch.admit(bad_seq, b"x")
        with self.assertRaises(InvalidArguments):
            ch.admit(0, "x")

    def test_rejects_oversized_single_chunk(self):
        ch = InputChannel()
        with self.assertRaises(InvalidArguments):
            ch.admit(0, b"x" * (MAX_CHUNK + 1))
        # 恰好上限可以接受。
        self.assertEqual(ch.admit(0, b"x" * MAX_CHUNK)["status"], "pending")

    # ---- 幂等重试与异内容拒绝 --------------------------------------------

    def test_same_seq_same_content_is_idempotent(self):
        r, w = self._pipe()
        ch = InputChannel()
        data = b"A" * 5000
        r1 = ch.admit(0, data)
        r2 = ch.admit(0, data)  # 模拟超时后的重试
        self.assertEqual(r1, r2)
        ch.request_close()
        _flush(ch, w)
        os.close(w)
        got = os.read(r, 65536)
        self.assertEqual(got, data)  # 绝不重复写入前半段

    def test_same_seq_different_content_rejected(self):
        ch = InputChannel()
        ch.admit(0, b"hello")
        with self.assertRaises(InvalidArguments):
            ch.admit(0, b"HELLO")
        # 被拒绝后，原片段仍是同一个（内容未被覆盖）。
        self.assertEqual(ch.receipts()[0]["size"], 5)

    def test_retry_after_partial_write_still_delivers_full_bytes(self):
        r, w = self._pipe()
        os.set_blocking(w, False)
        ch = InputChannel()
        data = bytes(range(256)) * 20  # 5120 字节
        ch.admit(0, data)
        # 模拟管道满时的部分写入（reader 尚未读取）。
        first = ch.write_ready(w)
        self.assertIn(first, ("progress", "retry"))
        receipt = ch.receipts()[0]
        # 此时客户端按同序号同内容重试：仍返回同一回执，不重复排队。
        same = ch.admit(0, data)
        self.assertEqual(same["written"], receipt["written"])
        # reader 排空后继续写到完整。
        def drain_reader():
            buf = b""
            while True:
                try:
                    piece = os.read(r, 4096)
                except BlockingIOError:
                    return buf
                if not piece:
                    return buf
                buf += piece

        os.set_blocking(r, False)
        received = b""
        for _ in range(100):
            while ch.write_ready(w) == "progress":
                pass
            received += drain_reader()
            if ch.receipts()[0]["status"] == "delivered":
                break
        received += drain_reader()
        ch.request_close()
        self.assertEqual(ch.write_ready(w), "eof")
        self.assertEqual(received, data)

    # ---- 顺序与完整字节 --------------------------------------------------

    def test_must_be_contiguous_from_zero(self):
        ch = InputChannel()
        with self.assertRaises(InvalidArguments):
            ch.admit(1, b"x")          # 必须从 0 开始
        ch.admit(0, b"a")
        with self.assertRaises(InvalidArguments):
            ch.admit(2, b"c")          # 跳过 1
        ch.admit(1, b"b")             # 连续：允许

    def test_bytes_delivered_in_fragment_order_with_boundaries(self):
        r, w = self._pipe()
        ch = InputChannel()
        parts = [b"", b"first-", b"s" * 60_000, bytes([0, 255]), b"tail"]
        for i, p in enumerate(parts):
            ch.admit(i, p)
        ch.request_close()
        _flush(ch, w)
        os.close(w)

        received = b""
        while True:
            piece = os.read(r, 65536)
            if not piece:
                break
            received += piece
        self.assertEqual(received, b"".join(parts))
        for rcpt, p in zip(ch.receipts(), parts):
            self.assertEqual(rcpt["status"], "delivered")
            self.assertEqual(rcpt["written"], len(p))

    def test_partial_writer_completes_exact_bytes(self):
        # writer 每次只接受 7 字节：完整字节必须最终全部送达。
        r, w = self._pipe()
        ch = InputChannel()
        ch.admit(0, b"abcdefghij")
        ch.admit(1, b"KLMNOPQRST")

        def limited_writer(fd, payload):
            return os.write(fd, payload[:7])

        # 前两次排空时不关闭：写完两个片段后应进入 retry（等更多片段）。
        saw = set()
        for _ in range(100):
            a = ch.write_ready(w, writer=limited_writer)
            saw.add(a)
            if a == "retry":
                break
        self.assertIn("retry", saw)
        self.assertTrue(all(x["status"] == "delivered" for x in ch.receipts()))
        self.assertFalse(ch.closed)

        # 再请求结束：空队列立即 EOF。
        ch.request_close()
        self.assertEqual(ch.write_ready(w, writer=limited_writer), "eof")
        os.close(w)
        received = b""
        while True:
            piece = os.read(r, 65536)
            if not piece:
                break
            received += piece
        self.assertEqual(received, b"abcdefghijKLMNOPQRST")

    # ---- 背压上限 --------------------------------------------------------

    def test_pending_total_limit_rejects_then_allows_after_drain(self):
        r, w = self._pipe()
        os.set_blocking(w, False)
        ch = InputChannel()
        size = 60_000
        seq = 0
        accepted = 0
        while True:
            try:
                ch.admit(seq, b"q" * size)
            except InvalidArguments:
                break
            seq += 1
            accepted += 1
            self.assertLessEqual(accepted * size, MAX_PENDING)
        # 17*60_000 = 1_020_000 <= 1MiB；第 18 片会超限被拒。
        self.assertEqual(accepted, MAX_PENDING // size)
        # 同序号同内容重试在拒收期间仍是幂等成功的（已接受过的片段）。
        prev = ch.receipts()[-1]
        self.assertEqual(ch.admit(prev["seq"], b"q" * size)["seq"],
                         prev["seq"])
        # 异内容仍拒绝。
        with self.assertRaises(InvalidArguments):
            ch.admit(prev["seq"], b"r" * size)
        # 排空一部分后，新片段又能被接受。
        os.set_blocking(r, False)
        for _ in range(5):
            ch.write_ready(w)
            try:
                os.read(r, 200_000)
            except BlockingIOError:
                pass
        ch.admit(seq, b"z")

    # ---- 结束输入：先排空再 EOF -------------------------------------------

    def test_close_waits_for_accepted_fragments_before_eof(self):
        r, w = self._pipe()
        os.set_blocking(w, False)
        ch = InputChannel()
        ch.admit(0, b"x" * 60_000)
        ch.admit(1, b"y" * 60_000)
        ch.admit(2, b"z" * 60_000)
        info = ch.request_close()
        self.assertTrue(info["closing"])
        self.assertFalse(info["closed"])

        # close 之后新片段一律拒绝。
        with self.assertRaises(InvalidArguments):
            ch.admit(3, b"no")
        # EOF 尚未发送（片段还没写完）：reader 仍能读到数据而不是 0。
        os.set_blocking(r, False)
        drained = b""
        while True:
            a = ch.write_ready(w)
            try:
                drained += os.read(r, 65536)
            except BlockingIOError:
                pass
            if a == "eof":
                os.close(w)
                break
            self.assertIn(a, ("progress",))
        self.assertEqual(drained,
                         b"x" * 60_000 + b"y" * 60_000 + b"z" * 60_000)
        # EOF 已发。
        self.assertEqual(os.read(r, 4096), b"")

    def test_close_idempotent(self):
        ch = InputChannel()
        a = ch.request_close()
        b = ch.request_close()
        self.assertEqual(a, b)
        with self.assertRaises(InvalidArguments):
            ch.admit(0, b"x")

    # ---- 终结：取消 / 退出 / 管道断裂 -------------------------------------

    def test_finish_marks_undelivered_failed_keeps_delivered(self):
        r, w = self._pipe()
        ch = InputChannel()
        ch.admit(0, b"done")
        ch.admit(1, b"not yet")
        ch.admit(2, b"also pending")
        # 第 0 片正常送达。
        self.assertEqual(ch.write_ready(w), "progress")
        ch.finish("cancelled")
        rcpts = ch.receipts()
        self.assertEqual(rcpts[0]["status"], "delivered")
        self.assertEqual(rcpts[0]["written"], 4)
        for rcpt in rcpts[1:]:
            self.assertEqual(rcpt["status"], "failed")
            self.assertEqual(rcpt["reason"], "cancelled")
            self.assertEqual(rcpt["written"], 0)
        # 终态后不再接受片段，重复 finish 不覆盖原因。
        ch.finish("exited")
        self.assertEqual(ch.receipts()[1]["reason"], "cancelled")
        with self.assertRaises(InvalidArguments):
            ch.admit(3, b"x")

    def test_finish_records_partial_written_bytes(self):
        r, w = self._pipe()
        os.set_blocking(w, False)
        ch = InputChannel()
        ch.admit(0, b"p" * 60_000)
        ch.admit(1, b"q" * 60_000)
        ch.write_ready(w)  # 第一片约 64KiB 内基本写完
        ch.write_ready(w)  # 第二片只能写入管道剩余空间
        rcpt = ch.receipts()[1]
        self.assertEqual(rcpt["status"], "pending")
        self.assertGreater(rcpt["written"], 0)
        self.assertLess(rcpt["written"], 60_000)
        written = rcpt["written"]
        ch.finish("timed_out")
        rcpt = ch.receipts()[1]
        self.assertEqual(rcpt["status"], "failed")
        self.assertEqual(rcpt["reason"], "timed_out")
        self.assertEqual(rcpt["written"], written)

    def test_broken_pipe_fails_all_pending(self):
        r, w = os.pipe()
        os.close(r)  # reader 直接消失（手动管理，不挂 addCleanup）
        try:
            ch = InputChannel()
            ch.admit(0, b"abc")
            ch.admit(1, b"def")
            action = ch.write_ready(w)
            self.assertEqual(action, "broken")
            for rcpt in ch.receipts():
                self.assertEqual(rcpt["status"], "failed")
                self.assertEqual(rcpt["reason"], "broken_pipe")
            self.assertTrue(ch.closed)
            with self.assertRaises(InvalidArguments):
                ch.admit(2, b"x")
        finally:
            try:
                os.close(w)
            except OSError:
                pass


if __name__ == "__main__":
    unittest.main()
