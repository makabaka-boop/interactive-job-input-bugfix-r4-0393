"""作业终态与异常定义。

状态机：
    QUEUED  -> RUNNING -> {EXITED, TIMED_OUT, OUTPUT_LIMIT, CANCELLED}
    QUEUED  -> CANCELLED                     （排队中被取消，从未启动）

所有终态都是最终状态，作业只会被“第一次发生的事件”终结一次：
取消在作业自然退出之后才生效时，终态保持 EXITED，而不是 CANCELLED。
"""

from enum import Enum


class JobState(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    EXITED = "exited"            # 进程自然结束（无论退出码是否为 0）
    TIMED_OUT = "timed_out"      # 超过运行时间上限，进程组被杀死
    OUTPUT_LIMIT = "output_limit"  # stdout+stderr 合计字节超限，进程组被杀死
    CANCELLED = "cancelled"      # 被取消（排队中或运行中），进程组被杀死

    @property
    def terminal(self) -> bool:
        return self not in (JobState.QUEUED, JobState.RUNNING)


class JobError(Exception):
    """作业服务通用错误。"""


class UnknownCommand(JobError):
    """提交了注册表中不存在的命令名。"""


class InvalidArguments(JobError):
    """命令参数数量不对或不在允许范围内。"""


class JobNotFound(JobError):
    """作业 id 不存在。"""


class CancelTimeout(JobError):
    """等待取消完成超过了调用方给定的时限。"""
