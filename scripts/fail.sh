#!/bin/sh
# 预定义命令：向 stderr 输出一行，然后以固定码 7 退出。
echo "deliberate failure on stderr" >&2
exit 7
