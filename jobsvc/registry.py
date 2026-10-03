"""预定义测试命令注册表。

服务只接受这里登记过的命令名，参数经白名单规则校验后以 argv 数组形式
传给 execve（绝不经过 /bin/sh -c，也不接受任意 shell 文本）。

每个整数参数都有独立的取值范围，防止注入参数或制造资源滥用。
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass

from .states import InvalidArguments, UnknownCommand

# 脚本目录固定在仓库内，提交时无法改变。
SCRIPTS_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, "scripts")
)

_INT_RE = re.compile(r"^[0-9]{1,9}$")


@dataclass(frozen=True)
class IntParam:
    name: str
    lo: int
    hi: int

    def validate(self, raw: str) -> int:
        if not isinstance(raw, str) or not _INT_RE.match(raw):
            raise InvalidArguments(
                f"参数 {self.name} 必须是非负整数（最多 9 位），收到: {raw!r}"
            )
        value = int(raw)
        if not (self.lo <= value <= self.hi):
            raise InvalidArguments(
                f"参数 {self.name} 必须在 [{self.lo}, {self.hi}] 内，收到: {value}"
            )
        return value


@dataclass(frozen=True)
class CommandEntry:
    name: str
    script: str
    params: tuple[IntParam, ...] = ()

    def build_argv(self, args: list[str]) -> list[str]:
        if len(args) != len(self.params):
            raise InvalidArguments(
                f"命令 {self.name} 需要 {len(self.params)} 个参数，"
                f"收到 {len(args)} 个"
            )
        values = [p.validate(a) for p, a in zip(self.params, args)]
        script_path = os.path.join(SCRIPTS_DIR, self.script)
        return [script_path, *(str(v) for v in values)]


# 注册表：新增可执行的测试命令只能在此处声明。
_REGISTRY: dict[str, CommandEntry] = {
    e.name: e
    for e in (
        CommandEntry("stdin_echo", "stdin_echo.sh"),
        CommandEntry("stdin_wait", "stdin_wait.sh"),
        CommandEntry("echo", "echo.sh"),
        CommandEntry("fail", "fail.sh"),
        CommandEntry("slow", "slow.sh", (IntParam("seconds", 1, 3600),)),
        CommandEntry(
            "big_stdout", "big_stdout.sh",
            (IntParam("bytes", 0, 100_000_000),),
        ),
        CommandEntry(
            "big_stderr", "big_stderr.sh",
            (IntParam("bytes", 0, 100_000_000),),
        ),
        CommandEntry(
            "alternating", "alternating.sh",
            (IntParam("lines", 0, 1_000_000),),
        ),
        CommandEntry(
            "spawn", "spawn.sh",
            (IntParam("children", 0, 1000), IntParam("seconds", 1, 3600)),
        ),
        CommandEntry(
            "spawn_ignore", "spawn_ignore.sh",
            (IntParam("children", 0, 1000), IntParam("seconds", 1, 3600)),
        ),
        CommandEntry(
            "orphan_grandchild", "orphan_grandchild.sh",
            (IntParam("orphan_seconds", 1, 3600),
             IntParam("parent_seconds", 1, 3600)),
        ),
        CommandEntry("exit_race", "exit_race.sh"),
        CommandEntry(
            "blast", "blast.sh",
            (IntParam("children", 0, 1000),
             IntParam("bytes", 0, 100_000_000),
             IntParam("seconds", 1, 3600)),
        ),
    )
}


def get(name: str) -> CommandEntry:
    try:
        return _REGISTRY[name]
    except KeyError:
        raise UnknownCommand(f"未注册的命令: {name!r}") from None


def command_names() -> list[str]:
    return sorted(_REGISTRY)

INTERACTIVE = {"stdin_echo", "stdin_wait"}
