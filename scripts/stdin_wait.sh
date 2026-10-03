#!/bin/sh
# 等待给定秒数（默认 5，注册表限制 1..60）后退出，不读取 stdin。
# 用于验证：进程自然退出后，未被读取的输入片段得到明确失败回执。
secs="${1:-5}"
printf "waiting %s\n" "$secs"
sleep "$secs"
