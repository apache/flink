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
# Prints the CPU, core count and memory of the machine running the job. Intended as the first step of
# every CI job so build/test timings can be correlated with the hardware they ran on.
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
