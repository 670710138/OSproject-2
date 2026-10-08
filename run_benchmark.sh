#!/usr/bin/env bash
# ทดลองเปรียบเทียบ Traditional I/O กับ NIO : 1 และ 10 workers, แต่ละกรณี 3 รอบ
# ใช้: ./run_benchmark.sh [ขนาด MB=512] [port=9090]
set -e
SIZE_MB=${1:-512}; PORT=${2:-9090}
cd "$(dirname "$0")"
mkdir -p classes share
javac -d classes FileServer.java FileClient.java
[ -f share/test.bin ] || { echo "สร้างไฟล์ทดสอบ ${SIZE_MB} MB ..."; head -c $((SIZE_MB*1024*1024)) /dev/urandom > share/test.bin; }
echo "mode,workers,run,seconds,MBps,verify" > results.csv
for MODE in io nio; do
  java -cp classes FileServer $PORT share $MODE virtual > server_$MODE.log 2>&1 &
  SP=$!; sleep 1
  for W in 1 10; do
    java -cp classes FileClient localhost $PORT download test.bin $MODE $W 3 downloads | tee /dev/stderr | grep '^RESULT' | sed 's/^RESULT,//' >> results.csv
  done
  kill $SP; wait $SP 2>/dev/null || true
done
echo; echo "== ผลทั้งหมดอยู่ใน results.csv =="; cat results.csv
