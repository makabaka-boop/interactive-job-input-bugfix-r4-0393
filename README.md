# jobsvc —— Linux 本地作业服务

只执行**仓库内预定义**测试命令的本地作业服务。不接受任意 shell 文本，
不经过 `/bin/sh -c`；命令以 argv 数组 `execve`，参数经白名单校验。

- 最多 **2 个作业并发**，其余排队 FIFO 推进
- stdout / stderr 各自独立管道**分别流式保存**
- 每个作业有**运行时长上限**与 **stdout+stderr 合计字节上限**
- 取消 / 超时 / 输出超限 → `setsid` 新进程组 + `killpg` **整组杀死**
  （含子进程、被 init 收养的孙进程），SIGTERM 宽限期后升级 SIGKILL
- 任一路输出持续写入不会堵住另一路，也不会阻止取消/超时检查
- 客户端按**游标**（已读字节偏移）分页读取已保存输出
- 终态只落一次：**重复取消永远得到同一个终态**

## 目录

```
jobsvc/            服务实现（仅标准库，Python 3.10+）
  states.py        状态机与异常
  registry.py      预定义命令白名单 + 参数校验
  service.py
  server.py        Unix 域套接字服务端（行分隔 JSON）
  client.py        客户端 SDK
  cli.py           命令行
scripts/           预定义测试命令（白名单的唯一可执行目标）
tests/             33 个基础测试 + 33 个交互输入测试
```

## 快速开始

```bash
# 启动（默认套接字 /tmp/jobsvc.sock，权限 0600）
python -m jobsvc serve

# 另一个终端
python -m jobsvc commands                    # 查看可执行命令及参数范围
python -m jobsvc submit spawn 3 30 --timeout 10
python -m jobsvc list
python -m jobsvc status <JOB_ID>
python -m jobsvc tail  <JOB_ID> stderr       # 流式跟随一路输出
python -m jobsvc read  <JOB_ID> stdout --offset 4096
python -m jobsvc cancel <JOB_ID>
python -m jobsvc shutdown
```

套接字路径可用 `--socket` 或环境变量 `JOBSVC_SOCKET` 覆盖。

## 预定义命令

| 命令 | 参数 | 用途 |
|---|---|---|
| `stdin_echo` | — | stderr 打印 ready 后 `cat` 回显 stdin（交互输入） |
| `stdin_wait` | `seconds[1..60]` | 不读 stdin，睡眠后退出（未送达回执场景） |
| `echo` | — | stdout 一行，退出 0 |
| `fail` | — | stderr 一行，退出 7 |
| `slow` | `seconds[1..3600]` | 睡眠，用于超时/排队/取消 |
| `big_stdout` / `big_stderr` | `bytes[0..1e8]` | 单路海量输出 |
| `alternating` | `lines[0..1e6]` | 两路交替写（管道独立性） |
| `spawn` | `children,seconds` | 派生子进程并 wait（进程组回收） |
| `spawn_ignore` | `children,seconds` | 整组忽略 SIGTERM（SIGKILL 升级） |
| `orphan_grandchild` | `orphan_s,parent_s` | 被 init 收养的孙进程仍须被杀死 |
| `exit_race` | — | 50ms 退出（退出/取消竞争） |
| `blast` | `children,bytes,seconds` | 派生子进程 + 两路洪泛的混合压力 |

命令名不在注册表中（如 `rm`、`"slow 1; echo x"`）直接拒绝；
参数只接受受限范围内的非负整数字符串，无法注入 shell 语法。

## 状态机

```
queued  -> running -> {exited, timed_out, output_limit, cancelled}
queued  -> cancelled           # 排队中取消，作业永不启动
```

裁决顺序（监控线程每个 poll 周期）：**先观察进程是否自然退出，
再看取消 / 超时 / 超限**。因此“退出与取消竞争”的结果对观察顺序确定：

- 取消在进程仍存活时被看到 → `cancelled`（即使进程瞬间后也会退出）
- 自然退出先被看到 → `exited`，之后再取消保持 `exited`

终态快照中 `returncode` 为负表示被信号终止（信号号 = `-returncode`，
同时给出 `term_signal`）。排队等待时间不计入作业的运行时长上限，
计时从作业实际启动开始。

## 关键实现

- **进程组隔离**：`Popen(..., start_new_session=True)`（即 `setsid`），
  作业组长 pid == pgid；杀死一律 `os.killpg(pgid, SIGTERM)`，
  宽限期（默认 2s，测试中 0.3s）后整组 `SIGKILL`；最后 `waitpid`
  回收直接子进程，关闭两路管道句柄，释放并发槽位。
- **双管道不互相阻塞**：监控线程把两个管道 fd 置非阻塞，用单个
  `select.poll` 同时收取；stdout 写爆不影响 stderr 的读取与 EOF 判定，
  取消/超时事件最长一个 poll tick（100ms）内被看到。
- **输出上限**：合计字节数（两路相加）达到上限即触发 `output_limit`
  并杀死进程组；超限后继续排空管道（防止子进程写阻塞拖住收尾），
  但只保留合计上限以内的前缀供游标读取。`produced_total` 是实际产出，
  `retained_total` 是保存下来的字节数。
- **取消幂等**：排队中取消直接终结并从队列移除；运行中取消只置一个
  `Event`。终态只能落一次，重复取消/对已终态作业取消都返回同一快照
  （`finished_at`、`term_signal` 不变）。
- **游标读取**：`read(id, stream, offset, max_bytes)` 返回
  `data / next_offset / eof / produced / state`，offset 即已读字节数，
  可从任意偏移重复读；数据以 base64 走套接字，支持二进制内容。

## 线路协议

Unix 域流式套接字，每行一个 JSON 请求/响应（仅本机，文件权限 0600）：

`submit` / `status` / `list` / `wait` / `cancel` / `read` /
`input` / `close_input` / `input_receipts` / `shutdown`。
`wait` 在服务端阻塞到终态（或给定超时）；`shutdown` 会取消所有排队作业、
杀死全部运行中进程组并删除套接字。

## 测试

```bash
python -m unittest discover -s tests -v
```

33 个基础用例覆盖：白名单与参数校验、非零退出码、stderr 海量洪泛、
两路交替写、输出合计上限（单路与双路）、超时杀整组、SIGKILL 升级、
被收养孙进程回收、取消幂等、退出-取消竞争、排队作业取消、两槽 FIFO
推进、游标分页/重读一致性、运行中读取不阻塞、关闭时整组回收，
以及经真实 Unix 套接字的端到端流程。

另有 33 个交互输入用例（`tests/test_input*.py`）覆盖：同序号同内容
幂等重试不重复写入、异内容/跳号/超片拒绝、64KiB 单片与 1MiB 背压上限、
部分写入后完整字节与顺序、空片段、关闭后拒绝新增且排空后才 EOF、
输出与输入交错不互堵、取消/超时/退出/断管时未送达片段的 failed 回执与
原因、排队中写输入与排队中取消，以及经真实套接字/base64 的端到端流程。

## 交互输入

`stdin_echo` 与 `stdin_wait` 是注册的交互命令（stdin 为管道，其余命令
stdin 接 `/dev/null`，输入操作被拒绝）。

- **分片输送**：`write_input(id, seq, bytes)`（协议 op `input`，载荷
  base64）接受从 **0 开始连续**序号的片段；单片上限 **64KiB**，尚未写完
  的片段合计上限 **1MiB**（背压超限拒绝，稍后按同序号重试即可）。
- **幂等重试 / 冲突拒绝**：同 `seq` + 同内容重发返回**同一个片段回执**，
  绝不重复写入（因此“只写到一半就返回、客户端重试”不会把前半段写两次）；
  同 `seq` + 异内容、跳号一律拒绝。
- **回执**：`input_receipts(id)` 按 seq 返回每片的
  `seq / size / written / status / reason`。`status` 为
  `pending`（已接受，`written` 为管道已实际接受的部分字节）、
  `delivered`（`written == size`，全部字节确实写进管道）、
  `failed`（`reason` ∈ `cancelled` / `timed_out` / `output_limit` /
  `exited` / `broken_pipe` / `write_error`，`written` 为已写字节）。
  **取消、超时、进程退出或管道断裂后，未送达片段一定是 failed 并带原因**，
  不会谎报成功。
- **结束输入**：`close_input(id)` 之后拒绝新增片段，但监控线程会把
  **已接受片段按序全部写完**再关闭管道（EOF），不截断在途内容；
  `cat` 等程序因此能收到完整输入后自然退出。管道写端为**非阻塞**并由
  监控线程的同一个 `poll` 驱动，子进程不读取导致管道满时，输出读取与
  取消/超时检查（最长一个 poll tick）都不会被阻塞；两路输出先 EOF 而
  stdin 仍开时也继续 poll，不再漏掉 EOF 与取消。
