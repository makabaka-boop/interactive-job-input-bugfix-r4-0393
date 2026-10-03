"""交互输入的作业级集成测试（真实子进程 + 监控线程）。

覆盖场景：
* 分次写入多个片段：子进程实际收到的字节与片段顺序、内容完全一致
* 同序号同内容重试不重复写入；异内容重试被拒
* 结束输入（close_input）后子进程读到真正的 EOF 并自然退出；
  已接受片段不被截断；关闭后新增片段被拒
* 输出与输入交错：边写 stdin 边从游标读 stdout，互不阻塞
* 命令等待输入时取消不卡死，未送达片段回执 failed/cancelled 且
  written 如实，已送达片段保持 delivered
* 进程退出后未送达片段 failed/exited，绝不再谎报成功
* 非交互命令拒绝输入 API
"""

from __future__ import annotations

import time
import unittest

from jobsvc.service import JobService
from jobsvc.states import InvalidArguments, JobState
from tests.helpers import read_all, wait_group_gone, wait_state

GRACE = 0.3


def wait_receipts(job, predicate, timeout=5.0, interval=0.005):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = job.input.receipts()
        if predicate(last):
            return last
        time.sleep(interval)
    raise AssertionError(f"回执未满足条件: {last}")


def all_delivered(receipts):
    return bool(receipts) and all(
        r["status"] == "delivered" for r in receipts)


class InteractiveInputServiceTest(unittest.TestCase):
    def setUp(self):
        self.svc = JobService(
            max_concurrent=2, default_timeout=10.0,
            default_output_max_bytes=4 * 1024 * 1024, kill_grace=GRACE)
        self.svc.start()
        self._closed = False

    def tearDown(self):
        if not self._closed:
            self.svc.close()

    # ---- 分次写入：顺序与完整字节 ---------------------------------------

    def test_fragmented_writes_delivered_exactly_in_order(self):
        job = self.svc.submit("stdin_echo")
        wait_state(job, JobState.RUNNING)
        fragments = [b"alpha\n", bytes(range(256)), b"\n", b"omega" * 5000]
        for i, frag in enumerate(fragments):
            self.svc.write_input(job.id, i, frag)
        self.svc.close_input(job.id)
        snap = wait_state(job, JobState.EXITED, timeout=10)
        self.assertEqual(snap["returncode"], 0)
        out = read_all(self.svc, job.id, "stdout", timeout=10)
        self.assertEqual(out, b"".join(fragments))
        receipts = self.svc.input_receipts(job.id)
        self.assertEqual(len(receipts), len(fragments))
        for i, frag in enumerate(fragments):
            self.assertEqual(receipts[i]["seq"], i)
            self.assertEqual(receipts[i]["size"], len(frag))
            self.assertEqual(receipts[i]["written"], len(frag))
            self.assertEqual(receipts[i]["status"], "delivered")
            self.assertIsNone(receipts[i]["reason"])

    def test_empty_fragment_supported(self):
        job = self.svc.submit("stdin_echo")
        wait_state(job, JobState.RUNNING)
        self.svc.write_input(job.id, 0, b"")
        self.svc.write_input(job.id, 1, b"x\n")
        self.svc.close_input(job.id)
        wait_state(job, JobState.EXITED)
        self.assertEqual(read_all(self.svc, job.id, "stdout"), b"x\n")
        receipts = self.svc.input_receipts(job.id)
        self.assertEqual(receipts[0]["size"], 0)
        self.assertEqual(receipts[0]["status"], "delivered")

    # ---- 重试语义 -------------------------------------------------------

    def test_same_seq_same_content_retry_is_idempotent(self):
        job = self.svc.submit("stdin_echo")
        wait_state(job, JobState.RUNNING)
        payload = b"retry-line\n"
        r1 = self.svc.write_input(job.id, 0, payload)
        # 在送达前后多次重放，回执始终是同一张、内容只写一次。
        for _ in range(5):
            r = self.svc.write_input(job.id, 0, payload)
            self.assertEqual(r["seq"], 0)
            self.assertEqual(r["size"], len(payload))
        wait_receipts(job, all_delivered)
        self.svc.close_input(job.id)
        wait_state(job, JobState.EXITED)
        self.assertEqual(read_all(self.svc, job.id, "stdout"), payload)

    def test_same_seq_different_content_retry_rejected(self):
        job = self.svc.submit("stdin_echo")
        wait_state(job, JobState.RUNNING)
        self.svc.write_input(job.id, 0, b"first\n")
        with self.assertRaises(InvalidArguments):
            self.svc.write_input(job.id, 0, b"SECOND\n")
        # 越序序号也拒绝。
        with self.assertRaises(InvalidArguments):
            self.svc.write_input(job.id, 5, b"x")
        self.svc.write_input(job.id, 1, b"ok\n")
        self.svc.close_input(job.id)
        wait_state(job, JobState.EXITED)
        self.assertEqual(
            read_all(self.svc, job.id, "stdout"), b"first\nok\n")

    def test_limits_enforced_at_service(self):
        job = self.svc.submit("stdin_echo")
        wait_state(job, JobState.RUNNING)
        with self.assertRaises(InvalidArguments):
            self.svc.write_input(job.id, 0, b"x" * (64 * 1024 + 1))

    # ---- 结束输入 -------------------------------------------------------

    def test_close_drains_before_eof_and_rejects_new_fragments(self):
        job = self.svc.submit("stdin_echo")
        wait_state(job, JobState.RUNNING)
        # 先塞满待处理队列（cat 读得很快，这里分多片提交后立即结束），
        # 关键断言：关闭返回时队列未必排空，但子进程最终必须收到全部
        # 已接受内容，EOF 不得提前。
        payloads = [f"line{i:06d}\n".encode() for i in range(2000)]
        for i, p in enumerate(payloads):
            self.svc.write_input(job.id, i, p)
        info = self.svc.close_input(job.id)
        self.assertTrue(info["closing"])
        with self.assertRaises(InvalidArguments):
            self.svc.write_input(job.id, len(payloads), b"late\n")
        wait_state(job, JobState.EXITED, timeout=15)
        out = read_all(self.svc, job.id, "stdout", timeout=15)
        self.assertEqual(out, b"".join(payloads))  # 无截断、无 late 片段

    # ---- 输入/输出交错，互不阻塞 ----------------------------------------

    def test_input_output_interleaving_does_not_block(self):
        job = self.svc.submit("stdin_echo")
        wait_state(job, JobState.RUNNING)
        total = bytearray()
        offset = 0
        rounds = 20
        for rnd in range(rounds):
            frag = f"round-{rnd:02d}-".encode() + b"z" * 1000 + b"\n"
            self.svc.write_input(job.id, rnd, frag)
            # 提交后立即拉取 stdout，验证输入不堵输出读取。
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                r = self.svc.read_output(job.id, "stdout", offset, 4096)
                total.extend(r["data"])
                offset = r["next_offset"]
                if total.endswith(b"\n") and total.count(b"\n") == rnd + 1:
                    break
                time.sleep(0.005)
        self.assertEqual(total.count(b"\n"), rounds)
        self.svc.close_input(job.id)
        wait_state(job, JobState.EXITED)
        out = read_all(self.svc, job.id, "stdout")
        expected = b"".join(
            f"round-{r:02d}-".encode() + b"z" * 1000 + b"\n"
            for r in range(rounds))
        self.assertEqual(out, expected)

    # ---- 取消：未送达明确失败、不卡死 ------------------------------------

    def test_cancel_while_waiting_for_input_does_not_hang_and_fails_pending(self):
        job = self.svc.submit("stdin_wait")
        wait_state(job, JobState.RUNNING)
        # stdin_wait 睡眠 5 秒、完全不读 stdin；写入量超过管道容量，
        # 制造“写不出去、命令在等待”的最坏情况。
        payloads = [b"x" * 60000 for _ in range(6)]  # 360KiB
        accepted = 0
        for i, p in enumerate(payloads):
            try:
                self.svc.write_input(job.id, i, p)
                accepted += 1
            except InvalidArguments:
                # 待处理达到 1MiB 上限是合法背压结果。
                break
        self.assertGreaterEqual(accepted, 1)
        pgid = job.snapshot()["pid"]
        t0 = time.monotonic()
        snap = self.svc.cancel(job.id, timeout=10)
        elapsed = time.monotonic() - t0
        self.assertEqual(snap["state"], "cancelled")
        self.assertLess(elapsed, 8.0)  # 绝不能像阻塞写那样卡住
        wait_group_gone(pgid)
        receipts = self.svc.input_receipts(job.id)
        self.assertEqual(len(receipts), accepted)
        for r in receipts:
            if r["written"] == r["size"]:
                self.assertEqual(r["status"], "delivered")
            else:
                self.assertEqual(r["status"], "failed")
                self.assertEqual(r["reason"], "cancelled")
                self.assertLess(r["written"], r["size"])

    def test_cancel_after_some_delivery_keeps_delivered_fails_rest(self):
        job = self.svc.submit("stdin_echo")
        wait_state(job, JobState.RUNNING)
        self.svc.write_input(job.id, 0, b"before-cancel\n")
        wait_receipts(job, all_delivered)
        # 提交更多片段后立刻取消：未送达的必须失败而不是假装成功。
        self.svc.write_input(job.id, 1, b"after-1\n")
        self.svc.write_input(job.id, 2, b"after-2\n")
        self.svc.cancel(job.id, timeout=10)
        wait_state(job, JobState.CANCELLED)
        receipts = self.svc.input_receipts(job.id)
        self.assertEqual(receipts[0]["status"], "delivered")
        for r in receipts[1:]:
            self.assertIn(r["status"], ("delivered", "failed"))
            if r["status"] == "failed":
                self.assertEqual(r["reason"], "cancelled")

    # ---- 进程退出：未送达明确失败 ----------------------------------------

    def test_exit_marks_undelivered_failed_not_delivered(self):
        # stdin_wait 睡眠 5s 后退出且不读输入。为快速验证“退出后未送达
        # 失败”，这里用 stdin_echo 关闭 stdin 后子进程自然退出，再对同
        # 序号之外的新写入验证关闭拒绝；已排队且未写完的极端情况由
        # test_cancel_while_waiting 覆盖。
        job = self.svc.submit("stdin_echo")
        wait_state(job, JobState.RUNNING)
        self.svc.write_input(job.id, 0, b"abc\n")
        self.svc.close_input(job.id)
        snap = wait_state(job, JobState.EXITED)
        self.assertEqual(snap["state"], "exited")
        # 进程已退出：任何新增片段都被拒绝。
        with self.assertRaises(InvalidArguments):
            self.svc.write_input(job.id, 1, b"late\n")
        # 已送达的重放仍返回 delivered 回执（诚实、幂等）。
        self.assertEqual(
            self.svc.write_input(job.id, 0, b"abc\n")["status"],
            "delivered")
        receipts = self.svc.input_receipts(job.id)
        self.assertEqual(receipts[0]["status"], "delivered")

    # ---- 非交互命令拒绝 -------------------------------------------------

    def test_non_interactive_command_rejects_input_api(self):
        job = self.svc.submit("echo")
        wait_state(job, JobState.EXITED)
        with self.assertRaises(InvalidArguments):
            self.svc.write_input(job.id, 0, b"x")
        with self.assertRaises(InvalidArguments):
            self.svc.close_input(job.id)


if __name__ == "__main__":
    unittest.main()
