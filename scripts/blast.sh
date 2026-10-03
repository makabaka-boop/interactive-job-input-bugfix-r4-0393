#!/bin/sh
# 预定义命令：混合压力——派生 $1 个睡眠 $3 秒的子进程，
# 同时向 stderr 刷 $2 字节，再向 stdout 刷 $2 字节。
n="$1"
bytes="$2"
m="$3"
i=0
while [ "$i" -lt "$n" ]; do
	( sleep "$m" ) &
	i=$((i + 1))
done
head -c "$bytes" /dev/zero | tr '\000' 'e' >&2
head -c "$bytes" /dev/zero | tr '\000' 'o'
wait
