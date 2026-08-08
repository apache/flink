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
# Samples host load every INTERVAL seconds until killed, to detect contention on shared agents.
# Zero dependencies (reads /proc). The key metric is CPU %steal: time this vCPU was runnable but the
# hypervisor gave the physical core to another tenant -> the direct signal of an oversubscribed host.
# Also reports %busy, %iowait, an approximate primary-disk util%, and loadavg vs core count.
#
# Usage: sample_load.sh [interval_seconds] [output_file]
#

INTERVAL="${1:-5}"
OUT="${2:-/dev/stdout}"

# Direct all output at the target. For a real file, reopen fd 1 once (works even when backgrounded).
# For stdout keep the inherited fd: reopening /dev/stdout by path fails with ENXIO when this runs in
# the background and stdout is a pipe.
case "$OUT" in
    ""|"-"|/dev/stdout|/dev/fd/1) : ;;
    *) exec >> "$OUT" 2>&1 ;;
esac

if [ ! -r /proc/stat ]; then
    echo "sample_load: /proc/stat not readable (not Linux?); skipping."
    exit 0
fi

# cpu line fields after 'cpu': user nice system idle iowait irq softirq steal
read_cpu() { awk '/^cpu /{print $2,$3,$4,$5,$6,$7,$8,$9}' /proc/stat; }
# sum of io_ticks (ms spent doing I/O, field 13) over whole block devices (not partitions).
# printf "%.0f" (not "print s+0") so large sums are plain integers, not awk scientific notation
# (e.g. 3.431e+09), which bash arithmetic cannot parse.
read_io() { awk '$3 ~ /^(sd[a-z]+|vd[a-z]+|xvd[a-z]+|nvme[0-9]+n[0-9]+)$/ {s+=$13} END{printf "%.0f\n", s+0}' /proc/diskstats; }

ncpu=$(nproc 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null || echo 1)
prev_cpu=($(read_cpu)); prev_io=$(read_io); prev_io=${prev_io:-0}
echo "# sample_load interval=${INTERVAL}s cores=${ncpu} -- watch 'steal' (>0 => contended host)"

while :; do
    sleep "$INTERVAL"
    cur_cpu=($(read_cpu)); cur_io=$(read_io); cur_io=${cur_io:-0}
    total=0
    for i in 0 1 2 3 4 5 6 7; do
        d[$i]=$(( ${cur_cpu[$i]} - ${prev_cpu[$i]} ))
        total=$(( total + ${d[$i]} ))
    done
    [ "$total" -le 0 ] && total=1
    idle=$(( ${d[3]} + ${d[4]} ))                 # idle + iowait
    busy=$(( 100 * (total - idle) / total ))
    iowait=$(( 100 * ${d[4]} / total ))
    steal=$(( 100 * ${d[7]} / total ))
    # io_ticks delta (ms) over the interval (INTERVAL*1000 ms) -> util%. Guard so a bad read can't
    # abort the whole sample line.
    util=0
    case "$cur_io$prev_io" in
        *[!0-9]*) : ;;
        *) util=$(( (cur_io - prev_io) / (INTERVAL * 10) )) ;;
    esac
    [ "$util" -gt 100 ] && util=100
    load=$(cut -d' ' -f1 /proc/loadavg)
    printf '%s busy=%3d%% iowait=%3d%% steal=%3d%% disk_util=%3d%% load=%s/%s\n' \
        "$(date +%T)" "$busy" "$iowait" "$steal" "$util" "$load" "$ncpu"
    prev_cpu=("${cur_cpu[@]}"); prev_io=$cur_io
done
