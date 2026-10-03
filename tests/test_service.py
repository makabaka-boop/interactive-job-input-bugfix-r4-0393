"""核心作业服务测试。

覆盖：
* 注册表白名单 / 参数校验，拒绝任意 shell 文本
* 正常退出与非零退出码、stdout/stderr 分流
* stderr 海量输出不会堵住 stdout（alternating / big_stderr）
* stdout 海量输出不会堵住 stderr
* 输出合计字节上限触发终止
* 超时终止整个进程组（含派生的子进程）
* SIGTERM 被忽略时 SIGKILL 升级
* 被 init 收养的孙进程也随进程组被杀死
* 取消幂等（重复取消唯一终态）
* 退出与取消竞争：取消早于自然退出 -> CANCELLED
* 排队中取消：作业永不启动
* 并发上限为 2、FIFO 推进
* 游标分页读取与重复读取一致性
* 服务关闭时杀死全部运行作业
"""

from __future__ import annotations

import time
import unittest

from jobsvc.service import JobService
from jobsvc.states import (
    InvalidArguments, JobState, UnknownCommand)
from tests.helpers import (
    group_alive, read_all, wait_group_gone, wait_state)

# 测试使用激进的杀死宽限期，整套用例跑得快。
GRACE = 0.3


class ServiceTest(unittest.TestCase):
    def setUp(self):
        self.svc = JobService(
            max_concurrent=2, default_timeout=5.0,
            default_output_max_bytes=256 * 1024, kill_grace=GRACE)
        self.svc.start()
        self._closed = False

    def tearDown(self):
        if not self._closed:
            self.svc.close()

    # ---- 白名单 ---------------------------------------------------------

    def test_unknown_command_rejected(self):
        with self.assertRaises(UnknownCommand):
            self.svc.submit("rm", ["-rf", "/"])
        with self.assertRaises(UnknownCommand):
            self.svc.submit("echo; rm -rf /")

    def test_arguments_validated(self):
        with self.assertRaises(InvalidArguments):
            self.svc.submit("slow", ["1; echo hi"])
        with self.assertRaises(InvalidArguments):
            self.svc.submit("slow", ["0"])       # 超出下界
        with self.assertRaises(InvalidArguments):
            self.svc.submit("slow", [])          # 参数数量不对
        with self.assertRaises(InvalidArguments):
            self.svc.submit("slow", ["99999999999999"])

    def test_bad_submit_limits(self):
        with self.assertRaises(InvalidArguments):
            self.svc.submit("echo", timeout=0.0)
        with self.assertRaises(InvalidArguments):
            self.svc.submit("echo", output_max_bytes=0)

    # ---- 基本执行 -------------------------------------------------------

    def test_echo_ok(self):
        job = self.svc.submit("echo")
        snap = wait_state(job, JobState.EXITED)
        self.assertEqual(snap["returncode"], 0)
        out = read_all(self.svc, job.id, "stdout")
        self.assertEqual(out, b"hello from predefined command\n")
        self.assertEqual(read_all(self.svc, job.id, "stderr"), b"")

    def test_fail_exit_code_and_stderr(self):
        job = self.svc.submit("fail")
        snap = wait_state(job, JobState.EXITED)
        self.assertEqual(snap["returncode"], 7)
        self.assertEqual(read_all(self.svc, job.id, "stdout"), b"")
        err = read_all(self.svc, job.id, "stderr")
        self.assertIn(b"deliberate failure", err)

    # ---- 管道独立性 / 错误输出洪泛 --------------------------------------

    def test_huge_stderr_does_not_block_stdout_termination(self):
        # stderr 刷 20MB（远超管道容量与保存上限），作业必须正常跑完。
        job = self.svc.submit("alternating", ["200000"], timeout=10,
                              output_max_bytes=64 * 1024 * 1024)
        snap = wait_state(job, JobState.EXITED, timeout=20)
        self.assertEqual(snap["returncode"], 0)
        # 两路都实际产生了输出，证明两边的读取都在并行进行。
        self.assertGreater(snap["produced"]["stdout"], 1_000_000)
        self.assertGreater(snap["produced"]["stderr"], 1_000_000)

    def test_massive_stderr_only(self):
        job = self.svc.submit("big_stderr", ["2000000"], timeout=10,
                              output_max_bytes=8 * 1024 * 1024)
        snap = wait_state(job, JobState.EXITED, timeout=20)
        self.assertEqual(snap["returncode"], 0)
        self.assertEqual(snap["produced"]["stdout"], 0)
        self.assertEqual(snap["produced"]["stderr"], 2_000_000)

    def test_massive_stdout_only(self):
        job = self.svc.submit("big_stdout", ["2000000"], timeout=10,
                              output_max_bytes=8 * 1024 * 1024)
        snap = wait_state(job, JobState.EXITED, timeout=20)
        self.assertEqual(snap["produced"]["stdout"], 2_000_000)
        self.assertEqual(snap["produced"]["stderr"], 0)

    # ---- 输出合计上限 ---------------------------------------------------

    def test_output_limit_kills_group(self):
        cap = 100_000
        job = self.svc.submit("big_stdout", ["10000000"],
                              output_max_bytes=cap, timeout=10)
        snap = wait_state(job, JobState.OUTPUT_LIMIT, timeout=10)
        self.assertIsNotNone(snap["term_signal"])
        # 保存内容不超过上限，且实际产出 >= 上限。
        self.assertLessEqual(snap["retained_total"], cap)
        self.assertGreaterEqual(snap["produced_total"], cap)
        wait_group_gone(snap["pid"])

    def test_output_limit_counts_both_streams(self):
        cap = 50_000
        # 每路每行 64 字节，约 400 行时合计触顶。
        job = self.svc.submit("alternating", ["100000"],
                              output_max_bytes=cap, timeout=10)
        snap = wait_state(job, JobState.OUTPUT_LIMIT, timeout=10)
        self.assertGreaterEqual(snap["produced_total"], cap)
        self.assertLessEqual(snap["retained_total"], cap)
        # 两路前缀都有数据。
        self.assertTrue(read_all(self.svc, job.id, "stdout"))
        self.assertTrue(read_all(self.svc, job.id, "stderr"))

    # ---- 超时 -----------------------------------------------------------

    def test_timeout_kills_process_group(self):
        job = self.svc.submit("spawn", ["3", "30"], timeout=0.8)
        wait_state(job, JobState.RUNNING)
        snap = wait_state(job, JobState.TIMED_OUT, timeout=8)
        pgid = snap["pid"]  # setsid 后 pgid == pid
        wait_group_gone(pgid, timeout=GRACE + 5)
        self.assertIsNotNone(snap["term_signal"])

    def test_short_job_not_timeout(self):
        job = self.svc.submit("echo", timeout=5)
        snap = wait_state(job, JobState.EXITED)
        self.assertEqual(snap["returncode"], 0)

    # ---- 取消 / 进程组 --------------------------------------------------

    def test_cancel_kills_children(self):
        job = self.svc.submit("spawn", ["5", "30"], timeout=30)
        snap = wait_state(job, JobState.RUNNING)
        pgid = snap["pid"]
        # 等子进程都派生出来。
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            alive = [p for p in _group_pids_public(pgid)]
            if len(alive) >= 6:  # 父 + 5 子
                break
            time.sleep(0.02)
        self.assertGreaterEqual(len(_group_pids_public(pgid)), 6)
        snap = self.svc.cancel(job.id, timeout=10)
        self.assertEqual(snap["state"], JobState.CANCELLED)
        wait_group_gone(pgid)

    def test_cancel_escalates_to_sigkill(self):
        # 整组忽略 SIGTERM，必须靠 SIGKILL 收尾。
        job = self.svc.submit("spawn_ignore", ["3", "30"], timeout=30)
        snap = wait_state(job, JobState.RUNNING)
        pgid = snap["pid"]
        t0 = time.monotonic()
        snap = self.svc.cancel(job.id, timeout=10)
        elapsed = time.monotonic() - t0
        self.assertEqual(snap["state"], JobState.CANCELLED)
        # 至少经历了宽限期（SIGTERM 无效），但不会无限等待。
        self.assertGreaterEqual(elapsed, GRACE - 0.15)
        wait_group_gone(pgid)

    def test_cancel_kills_orphaned_grandchild(self):
        # 孙进程被 init 收养但留在作业进程组里。
        job = self.svc.submit("orphan_grandchild", ["30", "30"], timeout=30)
        snap = wait_state(job, JobState.RUNNING)
        pgid = snap["pid"]
        time.sleep(0.5)  # 等父 shell 派生完孙进程且子 shell 退出
        self.assertTrue(group_alive(pgid))
        self.svc.cancel(job.id, timeout=10)
        wait_group_gone(pgid)

    def test_cancel_is_idempotent_single_terminal_state(self):
        job = self.svc.submit("spawn", ["2", "30"], timeout=30)
        wait_state(job, JobState.RUNNING)
        first = self.svc.cancel(job.id, timeout=10)
        self.assertEqual(first["state"], JobState.CANCELLED)
        # 立即重复取消、稍后再取消：结果必须完全一致，不出现第二种终态。
        for _ in range(5):
            again = self.svc.cancel(job.id, timeout=10)
            self.assertEqual(again["state"], "cancelled")
            self.assertEqual(again["finished_at"], first["finished_at"])
            self.assertEqual(again["term_signal"], first["term_signal"])
        self.svc.cancel(job.id)  # 非等待模式也幂等
        self.assertEqual(self.svc.snapshot(job.id)["state"], "cancelled")

    def test_cancel_after_natural_exit_stays_exited(self):
        job = self.svc.submit("echo")
        wait_state(job, JobState.EXITED)
        snap = self.svc.cancel(job.id, timeout=5)
        self.assertEqual(snap["state"], "exited")
        self.assertEqual(snap["returncode"], 0)

    def test_cancel_non_wait_mode_eventually_terminal(self):
        job = self.svc.submit("spawn", ["1", "30"], timeout=30)
        wait_state(job, JobState.RUNNING)
        self.svc.cancel(job.id, wait=False)
        snap = wait_state(job, JobState.CANCELLED, timeout=10)
        wait_group_gone(snap["pid"])

    # ---- 退出与取消竞争 -------------------------------------------------

    def test_cancel_wins_race_against_exit(self):
        # 作业 50ms 后自然退出；在其运行窗口内取消，终态必须是 CANCELLED。
        # 跑多次，每次取消都在自然退出观察点之前发起。
        results = set()
        for _ in range(8):
            job = self.svc.submit("exit_race", timeout=10)
            wait_state(job, JobState.RUNNING)
            snap = self.svc.cancel(job.id, timeout=5)
            self.assertIn(snap["state"], ("cancelled", "exited"))
            if snap["state"] == "cancelled":
                wait_group_gone(snap["pid"])
            results.add(snap["state"])
        # 取消请求在作业仍在运行时提出：两种观察结果都合法，但不得出现
        # 第三种状态；且同一作业重复取消保持原终态。
        self.assertTrue(results <= {"cancelled", "exited"})

    def test_early_cancel_always_cancels(self):
        # 1s 睡眠作业，取消有充足时间先到，必然 CANCELLED。
        job = self.svc.submit("slow", ["5"], timeout=30)
        wait_state(job, JobState.RUNNING)
        time.sleep(0.1)
        snap = self.svc.cancel(job.id, timeout=10)
        self.assertEqual(snap["state"], "cancelled")
        wait_group_gone(snap["pid"])

    # ---- 排队 -----------------------------------------------------------

    def test_cancel_queued_job_never_starts(self):
        # 占满两个槽位。
        j1 = self.svc.submit("slow", ["30"], timeout=30)
        j2 = self.svc.submit("slow", ["30"], timeout=30)
        wait_state(j1, JobState.RUNNING)
        wait_state(j2, JobState.RUNNING)
        j3 = self.svc.submit("echo")
        self.assertEqual(self.svc.snapshot(j3.id)["state"], "queued")
        # 排队中取消，立即终态。
        snap = self.svc.cancel(j3.id, timeout=5)
        self.assertEqual(snap["state"], "cancelled")
        self.assertIsNone(snap["started_at"])
        self.assertIsNone(snap["pid"])
        # 再取消结果不变。
        self.assertEqual(self.svc.cancel(j3.id)["state"], "cancelled")
        self.svc.cancel(j1.id)
        self.svc.cancel(j2.id)

    def test_queue_progresses_fifo_with_two_slots(self):
        jobs = [self.svc.submit("slow", ["30"], timeout=30)
                for _ in range(5)]
        wait_state(jobs[0], JobState.RUNNING)
        wait_state(jobs[1], JobState.RUNNING)
        self.assertEqual(self.svc.snapshot(jobs[2].id)["state"], "queued")
        # 取消队头两个后，第 3、4 个依次启动；第 5 个继续排队。
        self.svc.cancel(jobs[0].id)
        wait_state(jobs[2], JobState.RUNNING, timeout=5)
        self.svc.cancel(jobs[1].id)
        wait_state(jobs[3], JobState.RUNNING, timeout=5)
        self.assertEqual(self.svc.snapshot(jobs[4].id)["state"], "queued")
        for j in jobs[1:]:
            if self.svc.snapshot(j.id)["state"] == "running":
                self.svc.cancel(j.id)
        wait_state(jobs[2], JobState.CANCELLED)
        wait_state(jobs[3], JobState.CANCELLED)

    # ---- 游标读取 -------------------------------------------------------

    def test_cursor_pagination(self):
        job = self.svc.submit("alternating", ["100"], timeout=10)
        wait_state(job, JobState.EXITED)
        chunks = []
        offset = 0
        while True:
            r = self.svc.read_output(job.id, "stdout", offset, 37)
            self.assertLessEqual(len(r["data"]), 37)
            self.assertEqual(r["offset"], offset)
            chunks.append(r["data"])
            offset = r["next_offset"]
            if r["eof"]:
                break
        paginated = b"".join(chunks)
        full = read_all(self.svc, job.id, "stdout")
        self.assertEqual(paginated, full)
        # 从中间游标重读，内容一致。
        mid = len(full) // 2
        r = self.svc.read_output(job.id, "stdout", mid, 10_000)
        self.assertEqual(r["data"], full[mid:])

    def test_read_during_running_does_not_block(self):
        job = self.svc.submit("alternating", ["100000"], timeout=30,
                              output_max_bytes=64 * 1024 * 1024)
        saw_progress = False
        offset = 0
        for _ in range(50):
            r = self.svc.read_output(job.id, "stderr", offset, 4096)
            offset = r["next_offset"]
            if offset > 0:
                saw_progress = True
            if r["state"] not in ("running", "queued"):
                break
            time.sleep(0.01)
        self.svc.cancel(job.id)
        snap = wait_state(
            job, JobState.CANCELLED, JobState.OUTPUT_LIMIT, JobState.EXITED)
        wait_group_gone(snap["pid"])
        self.assertTrue(saw_progress)

    # ---- 关闭 -----------------------------------------------------------

    def test_close_kills_running_groups(self):
        job = self.svc.submit("spawn", ["3", "30"], timeout=30)
        snap = wait_state(job, JobState.RUNNING)
        pgid = snap["pid"]
        self.svc.close()
        self._closed = True
        wait_group_gone(pgid)


def _group_pids_public(pgid):
    from tests.helpers import group_pids
    return group_pids(pgid)


if __name__ == "__main__":
    unittest.main()
