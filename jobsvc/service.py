"""作业服务核心：受限命令提交、两槽位调度、每作业进程组监管。

关键设计：

* 每个作业启动时调用 ``setsid`` 进入新进程组/会话，取消/超时/超限一律
  ``killpg`` 整个进程组：命令派生的子进程、孙进程都逃不掉。
* stdout / stderr 各自独立管道，监控线程用非阻塞 I/O + ``poll`` 同时读
  两路，任何一路写爆都不会堵塞另一路，也不会阻止超时/取消检查。
* 触发杀死先发 SIGTERM 给整组，宽限期后升级 SIGKILL；判定依据是监控
  线程观察到的顺序——每轮循环先看进程是否自然退出，再看超时/取消/超限，
  因此“退出与取消竞争”的结果对观察顺序是确定的。
* 状态机只允许终态落一次，重复取消永远得到同一个终态。
"""

from __future__ import annotations

import os
import select
import signal
import subprocess
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field

from .input import InputChannel
from . import registry
from .states import CancelTimeout, InvalidArguments, JobNotFound, JobState

# 单路一次最多读取量；够大，减少被写满的管道反压次数。
_READ_CHUNK = 1 << 16
# poll 唤醒间隔上限，保证排队取消等事件最多延迟这么久被看到。
_POLL_TICK = 0.1

STDOUT = "stdout"
STDERR = "stderr"
_STREAMS = (STDOUT, STDERR)


@dataclass
class Job:
    """单个作业的全部状态。除监控线程外，所有读写都要持 ``lock``。"""

    id: str
    command: str
    argv: list[str]
    timeout: float
    output_max_bytes: int
    lock: threading.Lock = field(default_factory=threading.Lock)
    state: JobState = JobState.QUEUED
    queued_at: float = field(default_factory=time.monotonic)
    started_at: float | None = None
    finished_at: float | None = None
    deadline: float = 0.0
    pid: int | None = None
    pgid: int | None = None
    returncode: int | None = None
    term_signal: int | None = None
    # 两路各自保留“合计上限以内”的前缀，供游标读取。
    _retained: dict[str, bytearray] = field(default_factory=lambda: {
        STDOUT: bytearray(), STDERR: bytearray()})
    _retained_total: int = 0
    # 实际从管道排掉的字节数（超限后仍继续排空管道，但不再保存）。
    produced_total: int = 0
    produced: dict[str, int] = field(
        default_factory=lambda: {STDOUT: 0, STDERR: 0})
    eof: dict[str, bool] = field(
        default_factory=lambda: {STDOUT: False, STDERR: False})
    spawn_error: str | None = None
    cancel_event: threading.Event = field(default_factory=threading.Event)
    input: InputChannel = field(default_factory=InputChannel)
    terminal_event: threading.Event = field(default_factory=threading.Event)

    # ---- 快照与游标读取 -------------------------------------------------

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "id": self.id,
                "command": self.command,
                "state": self.state.value,
                "queued_at": self.queued_at,
                "started_at": self.started_at,
                "finished_at": self.finished_at,
                "pid": self.pid,
                "returncode": self.returncode,
                "term_signal": self.term_signal,
                "produced": dict(self.produced),
                "produced_total": self.produced_total,
                "retained_total": self._retained_total,
                "output_max_bytes": self.output_max_bytes,
                "timeout": self.timeout,
                "eof": dict(self.eof),
                "spawn_error": self.spawn_error,
            }

    def read_output(
        self, stream: str, offset: int, max_bytes: int = 1 << 16
    ) -> dict:
        """按游标（已读字节数）读取已保存的输出前缀。

        返回 {"data", "offset", "next_offset", "eof", "total"}。
        保存区只保留合计上限以内的前缀；终态后 eof=True，游标不再增长。
        """
        if stream not in _STREAMS:
            raise InvalidArguments(f"未知输出流: {stream!r}")
        if offset < 0 or max_bytes <= 0:
            raise InvalidArguments("offset 必须 >=0 且 max_bytes > 0")
        with self.lock:
            buf = self._retained[stream]
            if offset > len(buf):
                # 游标越过了保存区（只可能发生在合计上限截断处）：钉在末尾。
                offset = len(buf)
            end = min(len(buf), offset + max_bytes)
            data = bytes(buf[offset:end])
            return {
                "stream": stream,
                "data": data,
                "offset": offset,
                "next_offset": end,
                "total": len(buf),
                "produced": self.produced[stream],
                "eof": self.eof[stream] and end == len(buf),
                "state": self.state.value,
            }

    def wait_terminal(self, timeout: float | None = None) -> bool:
        return self.terminal_event.wait(timeout)


class JobService:
    """本地作业服务。默认最多两个并发作业。"""

    def __init__(
        self,
        max_concurrent: int = 2,
        default_timeout: float = 30.0,
        default_output_max_bytes: int = 1 << 20,
        kill_grace: float = 2.0,
    ):
        if max_concurrent < 1:
            raise ValueError("max_concurrent 必须 >= 1")
        self.max_concurrent = max_concurrent
        self.default_timeout = default_timeout
        self.default_output_max_bytes = default_output_max_bytes
        self.kill_grace = kill_grace

        self._jobs: dict[str, Job] = {}
        self._queue: deque[Job] = deque()
        self._active = 0
        self._lock = threading.Lock()
        self._wake = threading.Condition(self._lock)
        self._scheduler: threading.Thread | None = None
        self._stopping = False
        self._monitors: list[threading.Thread] = []

    # ---- 生命周期 -------------------------------------------------------

    def start(self) -> "JobService":
        if self._scheduler is not None:
            return self
        self._scheduler = threading.Thread(
            target=self._scheduler_loop, name="jobsvc-scheduler", daemon=True)
        self._scheduler.start()
        return self

    def close(self, wait: bool = True) -> None:
        """停止服务：取消所有排队作业，杀死全部运行中作业的进程组。"""
        with self._lock:
            self._stopping = True
            for job in list(self._jobs.values()):
                if job.state is JobState.QUEUED:
                    self._cancel_queued(job)
            self._wake.notify_all()
        # 排队作业已终结；运行中的作业整组杀死并等监控线程收尾。
        for job in list(self._jobs.values()):
            if job.state in (JobState.QUEUED, JobState.RUNNING):
                job.cancel_event.set()
                job.wait_terminal(timeout=self.kill_grace + 5)
        if self._scheduler is not None:
            self._scheduler.join(timeout=5)
        if wait:
            for t in list(self._monitors):
                t.join(timeout=self.kill_grace + 5)

    def __enter__(self) -> "JobService":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.close()

    # ---- 提交 / 查询 ---------------------------------------------------

    def submit(
        self,
        command: str,
        args: list[str] | None = None,
        timeout: float | None = None,
        output_max_bytes: int | None = None,
    ) -> Job:
        """提交一个注册表中的预定义命令。参数经白名单校验。"""
        entry = registry.get(command)  # 可能抛 UnknownCommand
        argv = entry.build_argv(list(args or []))  # 可能抛 InvalidArguments
        timeout = float(timeout if timeout is not None else self.default_timeout)
        output_max_bytes = int(
            output_max_bytes if output_max_bytes is not None
            else self.default_output_max_bytes)
        if not (0.1 <= timeout <= 86400):
            raise InvalidArguments("timeout 必须在 [0.1, 86400] 秒内")
        if not (1 <= output_max_bytes <= 1 << 30):
            raise InvalidArguments("output_max_bytes 必须在 [1, 1GiB] 内")

        job = Job(
            id=uuid.uuid4().hex,
            command=command,
            argv=argv,
            timeout=timeout,
            output_max_bytes=output_max_bytes,
        )
        with self._lock:
            if self._stopping:
                raise RuntimeError("服务正在关闭，不再接受作业")
            self._jobs[job.id] = job
            self._queue.append(job)
            self._wake.notify()
        return job

    def get(self, job_id: str) -> Job:
        try:
            return self._jobs[job_id]
        except KeyError:
            raise JobNotFound(f"作业不存在: {job_id}") from None

    def list_jobs(self) -> list[Job]:
        with self._lock:
            return list(self._jobs.values())

    def wait_for(self, job_id: str, timeout: float | None = None) -> Job:
        job = self.get(job_id)
        job.wait_terminal(timeout)
        return job

    def snapshot(self, job_id: str) -> dict:
        return self.get(job_id).snapshot()

    def read_output(self, job_id: str, stream: str, offset: int = 0,
                    max_bytes: int = 1 << 16) -> dict:
        return self.get(job_id).read_output(stream, offset, max_bytes)

    def write_input(self,job_id,seq,data):
        job=self.get(job_id)
        if job.command not in registry.INTERACTIVE:
            raise InvalidArguments('Command is not interactive')
        return job.input.admit(seq,data)

    def close_input(self,job_id):
        return self.get(job_id).input.request_close()

    def input_receipts(self,job_id):
        return self.get(job_id).input.receipts()

    # ---- 取消 -----------------------------------------------------------

    def cancel(self, job_id: str, wait: bool = True,
               timeout: float | None = 30.0) -> dict:
        """请求取消。幂等：重复调用永远返回同一个终态快照。

        * 排队中：直接落 CANCELLED，作业永远不会启动。
        * 运行中：置取消标志，监控线程在下一个 poll 周期对整组发 SIGTERM，
          宽限期后 SIGKILL；wait=True 时阻塞到终态落定。
        * 已终态：原样返回，绝不产生第二种结果。
        """
        job = self.get(job_id)
        with self._lock:
            if job.state is JobState.QUEUED:
                self._cancel_queued(job)
            elif job.state is JobState.RUNNING:
                job.cancel_event.set()
        if wait:
            if not job.wait_terminal(timeout):
                raise CancelTimeout(f"作业 {job_id} 在 {timeout}s 内未结束")
        return job.snapshot()

    def _cancel_queued(self, job: Job) -> None:
        # 调用方持 self._lock
        if job.state is not JobState.QUEUED:
            return
        job.state = JobState.CANCELLED
        job.finished_at = time.monotonic()
        try:
            self._queue.remove(job)
        except ValueError:
            pass
        job.eof[STDOUT] = job.eof[STDERR] = True
        job.cancel_event.set()
        job.input.finish('cancelled')
        job.terminal_event.set()

    # ---- 调度 -----------------------------------------------------------

    def _scheduler_loop(self) -> None:
        while True:
            started: list[Job] = []
            with self._lock:
                while not self._stopping and (
                    not self._queue or self._active >= self.max_concurrent
                ):
                    self._wake.wait(timeout=1.0)
                if self._stopping:
                    return
                now = time.monotonic()
                while self._queue and self._active < self.max_concurrent:
                    job = self._queue.popleft()
                    # 排队期间可能已被取消（理论上已出队，双保险）。
                    if job.state is not JobState.QUEUED:
                        continue
                    job.state = JobState.RUNNING
                    job.started_at = now
                    job.deadline = now + job.timeout
                    self._active += 1
                    started.append(job)
            for job in started:
                t = threading.Thread(
                    target=self._monitor, args=(job,),
                    name=f"jobsvc-monitor-{job.id[:8]}", daemon=True)
                with self._lock:
                    self._monitors.append(t)
                t.start()
    def _release_slot(self) -> None:
        with self._lock:
            self._active -= 1
            self._wake.notify()

    # ---- 单作业监管 -----------------------------------------------------

    def _monitor(self, job: Job) -> None:
        """运行单个作业直到终态。全程非阻塞读取两路管道并实施限制。"""
        try:
            proc = subprocess.Popen(
                job.argv,
                stdin=subprocess.PIPE if job.command in registry.INTERACTIVE else subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,  # setsid：新会话+新进程组
                close_fds=True,
                bufsize=0,
                cwd=registry.SCRIPTS_DIR,
            )
        except OSError as exc:  # 脚本丢失等：理论上不应发生
            self._finish_spawn_error(job, exc)
            return

        streams = (proc.stdout, proc.stderr)
        fds = {streams[0].fileno(): (0, STDOUT, streams[0]),
               streams[1].fileno(): (1, STDERR, streams[1])}
        poll = select.poll()
        for fd in fds:
            os.set_blocking(fd, False)
            poll.register(fd, select.POLLIN)
        open_fds = 2
        input_fd=proc.stdin.fileno() if proc.stdin else None
        input_registered=False
        if input_fd is not None:
            os.set_blocking(input_fd,True)


        with job.lock:
            job.pid = proc.pid
            job.pgid = os.getpgid(proc.pid)

        kill_reason: JobState | None = None
        kill_deadline: float | None = None
        sigkill_sent = False

        try:
            while True:
                now = time.monotonic()
                wait = _POLL_TICK
                if kill_deadline is not None:
                    wait = min(wait, max(0.0, kill_deadline - now))
                elif job.deadline:
                    wait = min(wait, max(0.0, job.deadline - now))
                # 取消标志尽快被看到
                if job.cancel_event.is_set():
                    wait = min(wait, 0.01)

                if input_fd is not None:
                    needed=job.input.has_work()
                    if needed and not input_registered:
                        poll.register(input_fd,select.POLLOUT)
                        input_registered=True
                    elif not needed and input_registered:
                        poll.unregister(input_fd)
                        input_registered=False
                events = poll.poll(wait * 1000) if open_fds else []
                for fd, _ev in events:
                    if fd==input_fd:
                        if job.input.drain(fd):
                            poll.unregister(fd)
                            input_registered=False
                            proc.stdin.close()
                            input_fd=None
                        continue
                    idx, name, fh = fds[fd]
                    try:
                        chunk = os.read(fd, _READ_CHUNK)
                    except BlockingIOError:
                        continue
                    except OSError:
                        chunk = b""
                    if chunk:
                        self._ingest(job, name, chunk)
                    else:
                        poll.unregister(fd)
                        fh.close()
                        open_fds -= 1
                        with job.lock:
                            job.eof[name] = True

                rc = proc.poll()

                # 先看自然退出，再看杀死条件：观察顺序即裁决顺序。
                if kill_reason is None:
                    if rc is None:
                        now = time.monotonic()
                        if job.cancel_event.is_set():
                            kill_reason = JobState.CANCELLED
                        elif now >= job.deadline:
                            kill_reason = JobState.TIMED_OUT
                        elif self._over_output_limit(job):
                            kill_reason = JobState.OUTPUT_LIMIT
                        if kill_reason is not None:
                            kill_deadline = time.monotonic() + self.kill_grace
                            self._signal_group(job, signal.SIGTERM)
                else:
                    if rc is None and not sigkill_sent and time.monotonic() >= kill_deadline:
                        self._signal_group(job, signal.SIGKILL)
                        sigkill_sent = True
                        kill_deadline = time.monotonic() + self.kill_grace

                if (rc is not None or kill_reason is not None) and input_fd is not None:
                    job.input.finish('exited' if kill_reason is None else kill_reason.value)
                    if input_registered:poll.unregister(input_fd)
                    proc.stdin.close()
                    input_fd=None
                    input_registered=False
                if rc is not None and open_fds == 0:
                    break
                if open_fds == 0 and not events:
                    # 两路管道都已 EOF 但进程还在（极少见）：避免空转。
                    time.sleep(min(_POLL_TICK, 0.02))
        finally:
            # 无论如何确保进程被回收、句柄被关闭、槽位被释放。
            if proc.poll() is None:
                self._signal_group(job, signal.SIGKILL)
            try:
                proc.wait(timeout=self.kill_grace + 5)
            except subprocess.TimeoutExpired:
                pass
            if proc.stdin:
                proc.stdin.close()
            job.input.finish('exited' if kill_reason is None else kill_reason.value)
            for fh in streams:
                try:
                    fh.close()
                except OSError:
                    pass

        self._finalize(job, proc, kill_reason)
        self._release_slot()

    # ---- 监控辅助 -------------------------------------------------------

    def _ingest(self, job: Job, stream: str, chunk: bytes) -> None:
        with job.lock:
            n = len(chunk)
            job.produced[stream] += n
            job.produced_total += n
            room = job.output_max_bytes - job._retained_total
            if room > 0:
                job._retained[stream].extend(chunk[:room])
                job._retained_total += min(room, n)

    def _over_output_limit(self, job: Job) -> bool:
        with job.lock:
            return job.produced_total >= job.output_max_bytes

    @staticmethod
    def _signal_group(job: Job, sig: int) -> None:
        """对整个进程组发信号；组已经消失不算错误。"""
        with job.lock:
            pgid = job.pgid
        if pgid is None:
            return
        try:
            os.killpg(pgid, sig)
        except ProcessLookupError:
            pass
        except OSError:
            # 兜底：组信号失败时直接打主进程
            with job.lock:
                pid = job.pid
            if pid is not None:
                try:
                    os.kill(pid, sig)
                except ProcessLookupError:
                    pass

    def _finalize(self, job: Job, proc: subprocess.Popen,
                  kill_reason: JobState | None) -> None:
        with job.lock:
            job.returncode = proc.returncode
            if proc.returncode is not None and proc.returncode < 0:
                job.term_signal = -proc.returncode
            job.state = kill_reason if kill_reason is not None else JobState.EXITED
            job.finished_at = time.monotonic()
            job.eof[STDOUT] = job.eof[STDERR] = True
            job.terminal_event.set()

    def _finish_spawn_error(self, job: Job, exc: OSError) -> None:
        with job.lock:
            job.state = JobState.EXITED
            job.returncode = 127
            job.input.finish('spawn_error')
            job.spawn_error = str(exc)
            job.started_at = job.started_at or time.monotonic()
            job.finished_at = time.monotonic()
            job.eof[STDOUT] = job.eof[STDERR] = True
            job.terminal_event.set()
        self._release_slot()
