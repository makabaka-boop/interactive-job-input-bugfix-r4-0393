"""Unix socket 端到端测试：服务器 + 真实客户端走套接字协议。"""

from __future__ import annotations

import os
import tempfile
import time
import unittest

from jobsvc.client import JobClient, ServiceError
from jobsvc.server import UnixServer
from jobsvc.service import JobService
from jobsvc.states import JobState
from tests.helpers import group_alive, read_all, wait_group_gone, wait_state


class ServerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="jobsvc-test-")
        self.sock_path = os.path.join(self.tmp, "svc.sock")
        self.svc = JobService(max_concurrent=2, kill_grace=0.3,
                              default_timeout=5,
                              default_output_max_bytes=256 * 1024)
        self.server = UnixServer(self.sock_path, self.svc,
                                 accept_timeout=0.2).start()
        self._server_closed = False
        self.assertTrue(os.path.exists(self.sock_path))
        mode = os.stat(self.sock_path).st_mode & 0o777
        self.assertEqual(mode, 0o600)

    def tearDown(self):
        if not self._server_closed:
            self.server.close()

    def test_submit_status_read_e2e(self):
        with JobClient(self.sock_path) as c:
            job = c.submit("alternating", ["50"])
            job = c.wait(job["id"], timeout=10)
            self.assertEqual(job["state"], "exited")
            self.assertEqual(job["returncode"], 0)
            stdout = b"".join(c.iter_output(job["id"], "stdout"))
            stderr = b"".join(c.iter_output(job["id"], "stderr"))
            self.assertEqual(stdout.count(b"stdout line"), 50)
            self.assertEqual(stderr.count(b"stderr line"), 50)
            self.assertEqual(c.status(job["id"])["state"], "exited")

    def test_unknown_command_over_socket(self):
        with JobClient(self.sock_path) as c:
            with self.assertRaises(ServiceError) as cm:
                c.submit("sh", ["-c", "echo pwned"])
            self.assertEqual(cm.exception.type_name, "UnknownCommand")

    def test_concurrency_limit_and_queued_cancel_e2e(self):
        with JobClient(self.sock_path) as c:
            a = c.submit("slow", ["30"], timeout=30)
            b = c.submit("slow", ["30"], timeout=30)
            c.wait(a["id"], timeout=0.1)
            self.assertEqual(c.status(a["id"])["state"], "running")
            self.assertEqual(c.status(b["id"])["state"], "running")
            d = c.submit("echo")
            self.assertEqual(c.status(d["id"])["state"], "queued")
            snap = c.cancel(d["id"])
            self.assertEqual(snap["state"], "cancelled")
            self.assertIsNone(snap["pid"])
            # 重复取消，同一结果。
            self.assertEqual(c.cancel(d["id"])["state"], "cancelled")
            c.cancel(a["id"])
            c.cancel(b["id"])
            c.wait(a["id"], timeout=10)
            c.wait(b["id"], timeout=10)

    def test_cancel_streaming_group_e2e(self):
        with JobClient(self.sock_path) as c:
            job = c.submit("spawn", ["4", "30"], timeout=30)
            time.sleep(0.5)
            running = c.status(job["id"])
            self.assertEqual(running["state"], "running")
            pgid = running["pid"]
            snap = c.cancel(job["id"], timeout=10)
            self.assertEqual(snap["state"], "cancelled")
            wait_group_gone(pgid)

    def test_output_limit_e2e(self):
        with JobClient(self.sock_path) as c:
            job = c.submit("big_stderr", ["5000000"],
                           output_max_bytes=4096, timeout=10)
            final = c.wait(job["id"], timeout=10)
            self.assertEqual(final["state"], "output_limit")
            total = 0
            offset = 0
            while True:
                r = c.read(job["id"], "stderr", offset, 1024)
                total += len(r["data"])
                offset = r["next_offset"]
                if r["eof"]:
                    break
            self.assertLessEqual(total, 4096)
            self.assertGreaterEqual(final["produced_total"], 4096)

    def test_two_clients_can_cancel_each_others_job(self):
        with JobClient(self.sock_path) as c1, JobClient(self.sock_path) as c2:
            job = c1.submit("slow", ["30"], timeout=30)
            time.sleep(0.3)
            snap = c2.cancel(job["id"])  # 另一个连接发起取消
            self.assertEqual(snap["state"], "cancelled")
            wait_group_gone(snap["pid"])
            # 原连接看到一致终态。
            self.assertEqual(c1.status(job["id"])["state"], "cancelled")

    def test_timeout_e2e(self):
        with JobClient(self.sock_path) as c:
            job = c.submit("slow", ["30"], timeout=0.5)
            final = c.wait(job["id"], timeout=8)
            self.assertEqual(final["state"], "timed_out")
            wait_group_gone(final["pid"])

    def test_input_e2e_fragments_retries_close_and_receipts(self):
        with JobClient(self.sock_path) as c:
            job = c.submit("stdin_echo", timeout=10,
                           output_max_bytes=4 * 1024 * 1024)
            time.sleep(0.2)
            self.assertEqual(c.status(job["id"])["state"], "running")
            fragments = [b"line0\n", bytes(range(256)), b"tail" * 2000]
            for i, frag in enumerate(fragments):
                r = c.write_input(job["id"], i, frag)
                self.assertEqual(r["seq"], i)
                self.assertEqual(r["size"], len(frag))
            # 同序号同内容重放：同一回执，不重复发送。
            replay = c.write_input(job["id"], 1, fragments[1])
            self.assertEqual(replay["size"], len(fragments[1]))
            # 异内容重试：协议层错误。
            with self.assertRaises(ServiceError) as cm:
                c.write_input(job["id"], 1, b"different")
            self.assertEqual(cm.exception.type_name, "InvalidArguments")
            # 越序同样拒绝。
            with self.assertRaises(ServiceError):
                c.write_input(job["id"], 99, b"x")
            info = c.close_input(job["id"])
            self.assertTrue(info["closing"])
            # 关闭后拒绝新增。
            with self.assertRaises(ServiceError):
                c.write_input(job["id"], len(fragments), b"late")
            final = c.wait(job["id"], timeout=10)
            self.assertEqual(final["state"], "exited")
            stdout = b"".join(c.iter_output(job["id"], "stdout"))
            self.assertEqual(stdout, b"".join(fragments))
            receipts = c.input_receipts(job["id"])
            self.assertEqual(len(receipts), 3)
            for i, frag in enumerate(fragments):
                self.assertEqual(receipts[i]["written"], len(frag))
                self.assertEqual(receipts[i]["status"], "delivered")

    def test_input_cancel_e2e_undelivered_reports_failed(self):
        with JobClient(self.sock_path) as c:
            job = c.submit("stdin_wait", timeout=30)
            time.sleep(0.2)
            # stdin_wait 不读 stdin；多发片段制造积压。
            accepted = 0
            for i in range(8):
                try:
                    c.write_input(job["id"], i, b"q" * 60000)
                    accepted += 1
                except ServiceError:
                    break
            self.assertGreaterEqual(accepted, 1)
            t0 = time.time()
            snap = c.cancel(job["id"], timeout=10)
            self.assertLess(time.time() - t0, 8.0)  # 取消不卡死
            self.assertEqual(snap["state"], "cancelled")
            wait_group_gone(snap["pid"])
            receipts = c.input_receipts(job["id"])
            self.assertEqual(len(receipts), accepted)
            for r in receipts:
                if r["written"] < r["size"]:
                    self.assertEqual(r["status"], "failed")
                    self.assertEqual(r["reason"], "cancelled")

    def test_non_interactive_command_rejects_input_e2e(self):
        with JobClient(self.sock_path) as c:
            job = c.submit("echo")
            c.wait(job["id"], timeout=5)
            with self.assertRaises(ServiceError) as cm:
                c.write_input(job["id"], 0, b"x")
            self.assertEqual(cm.exception.type_name, "InvalidArguments")

    def test_list_and_shutdown(self):
        with JobClient(self.sock_path) as c:
            c.submit("echo")
            time.sleep(0.3)
            jobs = c.list_jobs()
            self.assertEqual(len(jobs), 1)
            c.shutdown()
        # 关闭后套接字应消失
        deadline = time.time() + 5
        while os.path.exists(self.sock_path) and time.time() < deadline:
            time.sleep(0.05)
        self.assertFalse(os.path.exists(self.sock_path))
        self._server_closed = True


if __name__ == "__main__":
    unittest.main()
