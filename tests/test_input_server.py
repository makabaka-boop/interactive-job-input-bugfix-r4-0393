"""交互输入的 Unix 套接字端到端测试。

覆盖经真实行分隔 JSON 协议（base64 载荷）的：
* 分次写入 + 关闭 EOF，子进程实际收到完整有序字节
* 同 seq 同内容重试（含新连接重试）返回同一回执、字节不重复
* 同 seq 异内容 / 乱序 / 超片 / 关闭后新增 → InvalidArguments 错误
* 输出与输入交错时，另一路游标读取不阻塞
* 取消等待输入的作业：未送达回执 failed，终态 cancelled 及时
* 进程退出后未送达回执 failed，不谎报成功
"""

from __future__ import annotations

import os
import tempfile
import time
import unittest

from jobsvc.client import JobClient, ServiceError
from jobsvc.server import UnixServer
from jobsvc.service import JobService
from jobsvc.states import JobState
from tests.helpers import wait_group_gone


def _pattern(total: int) -> bytes:
    return bytes((i * 31 + 7) & 0xFF for i in range(total))


def _read_to_eof(client, job_id, stream, timeout=15.0):
    out = bytearray()
    offset = 0
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        r = client.read(job_id, stream, offset, 16384)
        out.extend(r["data"])
        offset = r["next_offset"]
        if r["eof"]:
            return bytes(out)
        time.sleep(0.01)
    raise AssertionError(f"{stream} 未 EOF")


def _pump(client, job_id, data, chunk_size, *, close=True, retry=()):
    n = (len(data) + chunk_size - 1) // chunk_size
    for seq in range(n):
        part = data[seq * chunk_size:(seq + 1) * chunk_size]
        client.write_input(job_id, seq, part)
        if seq in retry:
            # 模拟请求超时后的重发：同序号同内容。
            again = client.write_input(job_id, seq, part)
            assert again["size"] == len(part)
    if close:
        client.close_input(job_id)


class InputServerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="jobsvc-input-test-")
        self.sock_path = os.path.join(self.tmp, "svc.sock")
        self.svc = JobService(max_concurrent=2, kill_grace=0.3,
                              default_timeout=15,
                              default_output_max_bytes=4 * 1024 * 1024)
        self.server = UnixServer(self.sock_path, self.svc,
                                 accept_timeout=0.2).start()
        self._closed = False

    def tearDown(self):
        if not self._closed:
            self.server.close()

    def test_split_writes_and_eof_e2e(self):
        data = _pattern(150_000)
        with JobClient(self.sock_path) as c:
            job = c.submit("stdin_echo")
            jid = job["id"]
            _pump(c, jid, data, 7777, retry={0, 5, 19})
            final = c.wait(jid, timeout=15)
            self.assertEqual(final["state"], "exited")
            self.assertEqual(_read_to_eof(c, jid, "stdout"), data)
            receipts = c.input_receipts(jid)
            self.assertEqual(len(receipts), 20)
            self.assertTrue(all(r["status"] == "delivered" for r in receipts))
            # 二进制安全：含任意字节，经 base64 往返无失真（已由比对覆盖）。

    def test_retry_from_other_connection_is_idempotent(self):
        with JobClient(self.sock_path) as c1, \
                JobClient(self.sock_path) as c2:
            job = c1.submit("stdin_echo")
            jid = job["id"]
            c1.write_input(jid, 0, b"alpha-")
            # 另一个连接（“重试的请求”）同序号同内容：成功且不重复
            # （此时可能已被管道取走，pending/delivered 都合法）。
            r = c2.write_input(jid, 0, b"alpha-")
            self.assertIn(r["status"], ("pending", "delivered"))
            self.assertEqual(r["size"], len(b"alpha-"))
            c1.write_input(jid, 1, b"beta")
            c2.close_input(jid)
            final = c1.wait(jid, timeout=10)
            self.assertEqual(final["state"], "exited")
            self.assertEqual(_read_to_eof(c1, jid, "stdout"),
                             b"alpha-beta")

    def test_conflict_gap_oversize_and_post_close_rejected(self):
        with JobClient(self.sock_path) as c:
            job = c.submit("stdin_echo")
            jid = job["id"]
            c.write_input(jid, 0, b"same")

            def expect_invalid(fn):
                with self.assertRaises(ServiceError) as cm:
                    fn()
                self.assertEqual(cm.exception.type_name, "InvalidArguments")

            expect_invalid(lambda: c.write_input(jid, 0, b"SAME"))
            expect_invalid(lambda: c.write_input(jid, 2, b"gap"))
            expect_invalid(lambda: c.write_input(jid, 1, b"x" * 65537))
            expect_invalid(lambda: c.write_input(jid, -1, b"neg"))
            c.close_input(jid)
            expect_invalid(lambda: c.write_input(jid, 1, b"late"))
            final = c.wait(jid, timeout=10)
            self.assertEqual(final["state"], "exited")
            self.assertEqual(_read_to_eof(c, jid, "stdout"), b"same")
            # 非法 base64 也被拒绝且不影响作业（协议层 JobError）。
            with self.assertRaises(ServiceError) as cm2:
                c._call({"op": "input", "id": jid, "seq": 9,
                         "data": "!!!not base64!!!"})
            self.assertEqual(cm2.exception.type_name, "JobError")

    def test_output_interleaving_does_not_block_e2e(self):
        data = _pattern(120_000)
        with JobClient(self.sock_path) as c:
            job = c.submit("stdin_echo")
            jid = job["id"]
            # 边提交边从另一连接读取输出，互不阻塞。
            with JobClient(self.sock_path) as reader:
                _pump(c, jid, data, 9000)
                stdout = _read_to_eof(reader, jid, "stdout")
                stderr = _read_to_eof(reader, jid, "stderr")
            self.assertEqual(stdout, data)
            self.assertEqual(stderr, b"ready\n")

    def test_cancel_waiting_input_fails_receipts_e2e(self):
        with JobClient(self.sock_path) as c:
            job = c.submit("stdin_wait", ["30"], timeout=60)
            jid = job["id"]
            seq = 0
            payload = b"c" * 60_000
            # 送入若干片段直到背压/上限。
            while seq < 30:
                try:
                    c.write_input(jid, seq, payload)
                    seq += 1
                except ServiceError:
                    break
            self.assertGreater(seq, 0)
            t0 = time.monotonic()
            snap = c.cancel(jid, timeout=10)
            self.assertLess(time.monotonic() - t0, 8)
            self.assertEqual(snap["state"], "cancelled")
            wait_group_gone(snap["pid"])
            receipts = c.input_receipts(jid)
            self.assertTrue(any(r["status"] == "failed" for r in receipts))
            for r in receipts:
                if r["status"] == "failed":
                    self.assertEqual(r["reason"], "cancelled")
                    self.assertLessEqual(r["written"], r["size"])

    def test_exit_undelivered_reports_failed_e2e(self):
        with JobClient(self.sock_path) as c:
            job = c.submit("stdin_wait", ["1"], timeout=10)
            jid = job["id"]
            seq = 0
            off = 0
            payload = b"e" * 300_000
            while off < len(payload):
                part = payload[off:off + 4096]
                try:
                    c.write_input(jid, seq, part)
                    seq += 1
                    off += len(part)
                except ServiceError:
                    time.sleep(0.02)
            final = c.wait(jid, timeout=10)
            self.assertEqual(final["state"], "exited")
            receipts = c.input_receipts(jid)
            self.assertTrue(any(r["status"] == "failed" for r in receipts))
            for r in receipts:
                if r["status"] == "failed":
                    self.assertIn(r["reason"], ("exited", "broken_pipe"))
            # stdin_wait 只在 stdout 打印一行等待提示，从不消费输入。
            self.assertEqual(_read_to_eof(c, jid, "stdout"),
                             b"waiting 1\n")

    def test_non_interactive_command_rejects_input_e2e(self):
        with JobClient(self.sock_path) as c:
            job = c.submit("echo")
            jid = job["id"]
            with self.assertRaises(ServiceError) as cm:
                c.write_input(jid, 0, b"hi")
            self.assertEqual(cm.exception.type_name, "InvalidArguments")
            c.wait(jid, timeout=10)


if __name__ == "__main__":
    unittest.main()
