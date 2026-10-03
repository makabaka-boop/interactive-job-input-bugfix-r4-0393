"""测试公共辅助。"""

from __future__ import annotations

import os
import time

from jobsvc.states import JobState


def wait_state(job, *states, timeout=15.0, interval=0.01):
    """等待作业进入给定状态之一，返回最终快照。"""
    deadline = time.monotonic() + timeout
    while True:
        snap = job.snapshot()
        if snap["state"] in {s.value for s in states}:
            return snap
        if time.monotonic() >= deadline:
            raise AssertionError(
                f"作业 {job.id} 超时仍未到达 {states}，当前: {snap}")
        time.sleep(interval)


def read_all(service, job_id, stream, timeout=15.0, chunk=4096):
    """用游标把一路已保存输出读到 EOF，返回 bytes。"""
    out = bytearray()
    offset = 0
    deadline = time.monotonic() + timeout
    while True:
        r = service.read_output(job_id, stream, offset, chunk)
        out.extend(r["data"])
        offset = r["next_offset"]
        if r["eof"]:
            return bytes(out)
        if time.monotonic() >= deadline:
            raise AssertionError(f"读取 {stream} 超时: {r}")
        time.sleep(0.01)


def group_pids(pgid):
    """扫描 /proc，返回当前仍在给定进程组的 pid 列表（含僵尸）。"""
    pids = []
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        try:
            with open(f"/proc/{name}/stat", "r") as f:
                fields = f.read().split()
            # (pid comm) state ppid pgrp ...，comm 可能含空格/括号，
            # 所以从右侧固定位置取：pgrp 是第 5 个字段。
            pgrp = int(fields[4])
        except (OSError, IndexError, ValueError):
            continue
        if pgrp == pgid:
            pids.append(int(name))
    return pids


def group_alive(pgid):
    """组内是否还有非僵尸进程（僵尸已被 init 托管，会自行消失）。"""
    for pid in group_pids(pgid):
        try:
            with open(f"/proc/{pid}/stat", "r") as f:
                fields = f.read().split()
            state = fields[2]
        except (OSError, IndexError):
            continue
        if state != "Z":
            return True
    return False


def wait_group_gone(pgid, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not group_alive(pgid):
            return
        time.sleep(0.02)
    raise AssertionError(
        f"进程组 {pgid} 仍有存活进程: {group_pids(pgid)}")


TERMINAL = (JobState.EXITED, JobState.TIMED_OUT,
            JobState.OUTPUT_LIMIT, JobState.CANCELLED)
