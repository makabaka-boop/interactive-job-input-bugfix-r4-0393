"""服务级交互输入测试：真实子进程 + 监控线程。

覆盖：
* 分次写入（含大于管道容量的输入）字节与顺序完全一致，回执最终 delivered
* 同序号同内容重试幂等（不重复写入），异内容拒绝
* 写入与 stdout/stderr 输出交错（边输入边读输出不互相阻塞）
* close_input 等已接受片段写完后才发 EOF，子进程收到完整输入后退出
* 等待输入的作业被取消：未送达片段 failed/reason=cancelled，终态及时落定
* 进程不读输入即退出：未送达片段 failed/reason=exited，不谎报成功
* 关闭后新增片段被拒绝
"""

from __future__ import annotations

import time
import unittest

from jobsvc.service import JobService
from jobsvc.states import InvalidArguments, JobState
from tests.helpers import read_all, wait_group_gone, wait_state

GRACE = 0.3


def _pattern(total: int) -> bytes:
    # 确定性的可校验字节流（非纯重复字节，能发现拼接错位）。
    return bytes((i * 31 + 7) & 0xFF for i in range(total))


def _chunk(data: bytes, size: int):
    for i in range(0, len(data), size):
        yield i // size, data[i:i + size]


def _pump_inputs(svc, job_id, data, chunk_size=64 * 1024,
                 close=True, retry_every=None, deadline=15.0):
    """把 data 按 chunk_size 分片提交；可选模拟同序号同内容重试。"""
    end = time.monotonic() + deadline
    receipts = []
    for seq, part in _chunk(data, chunk_size):
        while True:
            if time.monotonic() > end:
                raise AssertionError("提交输入超时（背压未解除）")
            try:
                r = svc.write_input(job_id, seq, part)
                break
            except InvalidArguments as exc:
                # 仅允许 1MiB 背压拒收，退避后幂等重试同一 seq+内容。
                if "1MiB" not in str(exc):
                    raise
                time.sleep(0.01)
        receipts.append(r)
        if retry_every is not None and seq % retry_every == 0:
            again = svc.write_input(job_id, seq, part)
            receipts.append(again)
    if close:
        svc.close_input(job_id)
    return receipts


class InputServiceTest(unittest.TestCase):
    def setUp(self):
        self.svc = JobService(
            max_concurrent=2, default_timeout=15.0,
            default_output_max_bytes=4 * 1024 * 1024, kill_grace=GRACE)
        self.svc.start()
        self._closed = False

    def tearDown(self):
        if not self._closed:
            self.svc.close()

    # ---- 分次写入：顺序与完整字节 ----------------------------------------

    def test_multi_write_order_and_bytes_with_eof(self):
        data = _pattern(300_000)  # > 管道容量，强制背压/部分写入
        job = self.svc.submit("stdin_echo", timeout=15)
        # 等 ready 标记，证明 stderr 输出与输入并存可用。
        self._wait_stderr_ready(job.id)
        _pump_inputs(self.svc, job.id, data, chunk_size=4096, close=True)

        snap = wait_state(job, JobState.EXITED, timeout=15)
        self.assertEqual(snap["returncode"], 0)
        out = read_all(self.svc, job.id, "stdout")
        self.assertEqual(out, data)  # cat 逐字节回显：顺序/内容必须完全一致

        receipts = self.svc.input_receipts(job.id)
        self.assertEqual(len(receipts), 74)  # ceil(300000/4096)
        for r in receipts:
            self.assertEqual(r["status"], "delivered")
            self.assertIsNone(r["reason"])
            self.assertEqual(r["written"], r["size"])
        # seq 连续且有序。
        self.assertEqual([r["seq"] for r in receipts],
                         list(range(len(receipts))))

    def test_single_chunk_max_size(self):
        data = bytes(range(256)) * 256  # 恰好 65536
        job = self.svc.submit("stdin_echo", timeout=10)
        self.svc.write_input(job.id, 0, data)
        self.svc.close_input(job.id)
        wait_state(job, JobState.EXITED, timeout=10)
        self.assertEqual(read_all(self.svc, job.id, "stdout"), data)

    def test_chunk_too_large_rejected(self):
        job = self.svc.submit("stdin_echo", timeout=10)
        with self.assertRaises(InvalidArguments):
            self.svc.write_input(job.id, 0, b"x" * 65537)
        self.svc.close_input(job.id)
        wait_state(job, JobState.EXITED, timeout=10)

    def test_non_contiguous_and_conflicting_seq_rejected(self):
        job = self.svc.submit("stdin_echo", timeout=10)
        self.svc.write_input(job.id, 0, b"abc")
        with self.assertRaises(InvalidArguments):
            self.svc.write_input(job.id, 2, b"??")
        with self.assertRaises(InvalidArguments):
            self.svc.write_input(job.id, 0, b"ABC")  # 异内容
        # 同内容重试幂等：子进程只收到一份 abc。
        self.assertEqual(
            self.svc.write_input(job.id, 0, b"abc")["status"], "pending")
        self.svc.write_input(job.id, 1, b"def")
        self.svc.close_input(job.id)
        wait_state(job, JobState.EXITED, timeout=10)
        self.assertEqual(read_all(self.svc, job.id, "stdout"), b"abcdef")

    # ---- 输出交错 --------------------------------------------------------

    def test_output_interleaves_with_blocked_input(self):
        # 先灌满输入（子进程 cat 边读边回显），同时持续读 stderr/stdout：
        # 任何一边都不能堵住另一边。
        data = _pattern(200_000)
        job = self.svc.submit("stdin_echo", timeout=15)
        self._wait_stderr_ready(job.id)

        _pump_inputs(self.svc, job.id, data, chunk_size=10_000, close=True)

        got = bytearray()
        offset = 0
        deadline = time.monotonic() + 15
        while True:
            r = self.svc.read_output(job.id, "stdout", offset, 8192)
            got.extend(r["data"])
            offset = r["next_offset"]
            if r["eof"]:
                break
            if time.monotonic() > deadline:
                raise AssertionError("等待 stdout EOF 超时")
            time.sleep(0.005)
        self.assertEqual(bytes(got), data)
        stderr = read_all(self.svc, job.id, "stderr")
        self.assertEqual(stderr, b"ready\n")

    # ---- 关闭语义 --------------------------------------------------------

    def test_close_then_new_write_rejected_and_eof_after_flush(self):
        job = self.svc.submit("stdin_echo", timeout=10)
        self.svc.write_input(job.id, 0, b"line1\n")
        self.svc.close_input(job.id)
        with self.assertRaises(InvalidArguments):
            self.svc.write_input(job.id, 1, b"late\n")
        snap = wait_state(job, JobState.EXITED, timeout=10)
        self.assertEqual(snap["returncode"], 0)
        self.assertEqual(read_all(self.svc, job.id, "stdout"), b"line1\n")
        receipts = self.svc.input_receipts(job.id)
        self.assertEqual(receipts[0]["status"], "delivered")

    def test_close_before_any_write_sends_immediate_eof(self):
        job = self.svc.submit("stdin_echo", timeout=10)
        self.svc.close_input(job.id)
        snap = wait_state(job, JobState.EXITED, timeout=10)
        self.assertEqual(snap["returncode"], 0)
        self.assertEqual(read_all(self.svc, job.id, "stdout"), b"")

    def test_close_waits_for_large_accepted_fragments(self):
        # 请求关闭时仍有大批已接受片段未写完：EOF 必须等它们全部送达。
        data = _pattern(700_000)
        job = self.svc.submit("stdin_echo", timeout=20)
        _pump_inputs(self.svc, job.id, data, chunk_size=64 * 1024,
                     close=True, retry_every=3)
        snap = wait_state(job, JobState.EXITED, timeout=20)
        self.assertEqual(snap["returncode"], 0)
        self.assertEqual(read_all(self.svc, job.id, "stdout"), data)
        for r in self.svc.input_receipts(job.id):
            self.assertEqual(r["status"], "delivered")

    # ---- 取消：未送达回执明确失败、终态及时 -------------------------------

    def test_cancel_while_waiting_input_fails_pending_and_terminal(self):
        # 先送入 ~3 倍管道容量的片段（cat 读得慢/被取消），再取消。
        payload = _pattern(300_000)
        job = self.svc.submit("stdin_wait", ["1"], timeout=30)
        # stdin_wait 不读 stdin；写入很快被管道背压拒绝，故只接受管道
        # 容量附近的片段，其余在客户端侧 1MiB 上限处等待。改为：接受我们能
        # 接受的，然后取消，验证已接受未送达片段得到 failed 回执。
        accepted = []
        seq = 0
        off = 0
        deadline = time.monotonic() + 3
        while off < len(payload) and time.monotonic() < deadline:
            part = payload[off:off + 4096]
            try:
                self.svc.write_input(job.id, seq, part)
                accepted.append((seq, part))
                seq += 1
                off += len(part)
            except InvalidArguments:
                time.sleep(0.02)
        self.assertTrue(accepted)
        snap = self.svc.cancel(job.id, timeout=10)
        self.assertEqual(snap["state"], "cancelled")
        wait_group_gone(snap["pid"])

        receipts = self.svc.input_receipts(job.id)
        self.assertEqual(len(receipts), len(accepted))
        for r in receipts:
            # stdin_wait 从不读取：没有任何字节能“完整送达”；至多部分写入。
            self.assertIn(r["status"], ("failed", "delivered"))
            if r["status"] == "failed":
                self.assertEqual(r["reason"], "cancelled")
                self.assertLess(r["written"], r["size"])
        self.assertTrue(any(r["status"] == "failed" for r in receipts))
        # 取消后拒绝新增。
        with self.assertRaises(InvalidArguments):
            self.svc.write_input(job.id, seq, b"late")

    def test_cancel_during_big_drain_does_not_hang(self):
        # 已 accepted 接近 1MiB 时取消：监控线程此前在非阻塞管道上工作，
        # 必须及时落终态，回执失败化。
        payload = b"z" * (900_000)
        job = self.svc.submit("stdin_wait", ["30"], timeout=60)
        _ = self.svc.snapshot(job.id)
        seq = 0
        off = 0
        while off < len(payload):
            part = payload[off:off + 60_000]
            try:
                self.svc.write_input(job.id, seq, part)
                seq += 1
                off += len(part)
            except InvalidArguments:
                break  # 达到 1MiB 待处理上限
        t0 = time.monotonic()
        snap = self.svc.cancel(job.id, timeout=10)
        self.assertLess(time.monotonic() - t0, 8)
        self.assertEqual(snap["state"], "cancelled")
        receipts = self.svc.input_receipts(job.id)
        self.assertTrue(any(r["status"] == "failed" for r in receipts))
        for r in receipts:
            if r["status"] == "failed":
                self.assertEqual(r["reason"], "cancelled")

    # ---- 进程自然退出：未送达回执失败（不谎报成功） -----------------------

    def test_exit_with_undelivered_input_fails_receipts(self):
        # 塞满 > 管道容量的片段，stdin_wait 睡 1s 退出且从不读取。
        payload = b"u" * 200_000
        job = self.svc.submit("stdin_wait", ["1"], timeout=10)
        seq = 0
        off = 0
        while off < len(payload):
            part = payload[off:off + 4096]
            try:
                self.svc.write_input(job.id, seq, part)
                seq += 1
                off += len(part)
            except InvalidArguments:
                time.sleep(0.02)
        snap = wait_state(job, JobState.EXITED, timeout=10)
        self.assertEqual(snap["returncode"], 0)
        receipts = self.svc.input_receipts(job.id)
        self.assertTrue(receipts)
        # 后段片段必然未送达：reason=exited（监控先看到进程退出）或
        # broken_pipe（先看到管道断裂），二者都明确表示未送达，绝不能是
        # 无 reason 的 delivered。
        statuses = {r["status"] for r in receipts}
        self.assertIn("failed", statuses)
        for r in receipts:
            if r["status"] == "failed":
                self.assertIn(r["reason"], ("exited", "broken_pipe"))
                self.assertLessEqual(r["written"], r["size"])
        with self.assertRaises(InvalidArguments):
            self.svc.write_input(job.id, seq + 1, b"late")

    # ---- 非交互命令 ------------------------------------------------------

    def test_input_rejected_for_non_interactive_command(self):
        job = self.svc.submit("echo", timeout=10)
        with self.assertRaises(InvalidArguments):
            self.svc.write_input(job.id, 0, b"hi")
        with self.assertRaises(InvalidArguments):
            self.svc.close_input(job.id)
        wait_state(job, JobState.EXITED)

    def _wait_stderr_ready(self, job_id, timeout=5.0):
        deadline = time.monotonic() + timeout
        got = bytearray()
        offset = 0
        while time.monotonic() < deadline:
            r = self.svc.read_output(job_id, "stderr", offset, 4096)
            got.extend(r["data"])
            offset = r["next_offset"]
            if b"ready" in got:
                return
            time.sleep(0.01)
        raise AssertionError("未等到 ready 标记")


if __name__ == "__main__":
    unittest.main()
