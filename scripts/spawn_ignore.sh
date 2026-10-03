#!/bin/sh
# 预定义命令：父进程与 $1 个子进程都忽略 SIGTERM，子进程睡眠 $2 秒。
# 用于验证宽限期后整组 SIGKILL 升级。
trap '' TERM
n="$1"
m="$2"
i=0
while [ "$i" -lt "$n" ]; do
	( trap '' TERM; sleep "$m" ) &
	i=$((i + 1))
done
wait
