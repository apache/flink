#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

#
# Prints the CPU, core count, memory and disk throughput of the machine running the job. Intended as
# the first step of every CI job so build/test timings can be correlated with the hardware they ran on.
#

echo "=============================================================================="
echo "Machine information"
echo "=============================================================================="

echo "---- CPU ----"
if command -v lscpu >/dev/null 2>&1; then
    lscpu | grep -E "^(Architecture|Model name|CPU\(s\)|Thread\(s\) per core|Core\(s\) per socket|Socket\(s\)):"
elif [ -r /proc/cpuinfo ]; then
    grep -E "^model name" /proc/cpuinfo | head -n 1
else
    sysctl -n machdep.cpu.brand_string 2>/dev/null || echo "CPU model: n/a"
fi
# getconf is portable (Linux + macOS); nproc additionally honours cgroup limits in containers
echo "Logical cores available: $(nproc 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null || echo 'n/a')"

echo "---- Memory ----"
if command -v free >/dev/null 2>&1; then
    free -h
elif [ -r /proc/meminfo ]; then
    grep -E "^(MemTotal|MemAvailable):" /proc/meminfo
else
    mem_bytes=$(sysctl -n hw.memsize 2>/dev/null) && echo "MemTotal: $((mem_bytes / 1024 / 1024)) MB" || echo "Memory: n/a"
fi

echo "---- Disk (working dir: $(pwd)) ----"
df -h . 2>/dev/null
# filesystem type (Linux df supports -T; ignored elsewhere)
df -T . 2>/dev/null | awk 'NR==2 {print "Filesystem type: " $2}'
if command -v lsblk >/dev/null 2>&1; then
    lsblk -d -o NAME,ROTA,SIZE,MODEL 2>/dev/null
fi
# Rough sequential write throughput incl. flush. O_DIRECT is often unsupported on overlayfs (CI runs in
# containers), so use conv=fdatasync; fall back to non-GNU dd (e.g. macOS) which lacks that option.
if command -v dd >/dev/null 2>&1; then
    disk_probe="./.machine_info_disktest"
    echo "Sequential write (1 GiB, incl. flush):"
    probe_out=$(dd if=/dev/zero of="$disk_probe" bs=1M count=1024 conv=fdatasync 2>&1)
    if [ $? -ne 0 ]; then
        probe_out=$(dd if=/dev/zero of="$disk_probe" bs=1m count=1024 2>&1)
    fi
    echo "$probe_out" | tail -n 1
    rm -f "$disk_probe"
fi

# Small-file / metadata I/O -- the workload maven-shade actually generates (unpack/repack thousands of
# tiny class files). Sequential MB/s says nothing about this; a slow-IOPS disk shows up here, not above.
# Zero deps: time creating then deleting many tiny files. GNU date gives ms; falls back to seconds*1000.
now_ms() { local d; d=$(date +%s%3N 2>/dev/null); case "$d" in ''|*[!0-9]*) d=$(( $(date +%s) * 1000 ));; esac; echo "$d"; }
iops_dir="./.machine_info_iops_$$"
mkdir -p "$iops_dir" 2>/dev/null
N=2000
t0=$(now_ms); i=0
while [ "$i" -lt "$N" ]; do printf 'x' > "$iops_dir/f$i"; i=$(( i + 1 )); done
sync 2>/dev/null
t1=$(now_ms)
rm -rf "$iops_dir"
t2=$(now_ms)
cr=$(( t1 - t0 )); [ "$cr" -le 0 ] && cr=1
dl=$(( t2 - t1 )); [ "$dl" -lt 0 ] && dl=0
echo "Small-file I/O: create ${N} tiny files in ${cr} ms ($(( N * 1000 / cr )) files/s), delete in ${dl} ms"
