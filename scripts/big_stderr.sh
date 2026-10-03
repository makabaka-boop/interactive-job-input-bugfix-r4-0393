#!/bin/sh
# 预定义命令：向 stderr 写入恰好 $1 个字节的 'y'（整数，0..104857600）。
head -c "$1" /dev/zero | tr '\000' 'y' >&2
