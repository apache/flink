/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.messages;

/**
 * Granularity of the thread dump collected via {@link
 * java.lang.management.ThreadMXBean#dumpAllThreads(boolean, boolean)}. Information about the lock
 * each thread is currently waiting on ({@link java.lang.management.ThreadInfo#getLockInfo()}) is
 * populated in both modes.
 */
public enum ThreadDumpMode {

    /**
     * {@code dumpAllThreads(false, false)}: stack traces only, no lock info (jstack without {@code
     * -l}). Negligible JVM pause.
     */
    LITE(false, false),

    /**
     * {@code dumpAllThreads(true, true)}: also collects locked monitors and j.u.c. synchronizers
     * (equivalent to {@code jstack -l}). Pauses the JVM in a safepoint for a duration that scales
     * with heap size and thread count -- seconds on large TaskManagers.
     */
    FULL(true, true);

    private final boolean lockedMonitors;
    private final boolean lockedSynchronizers;

    ThreadDumpMode(boolean lockedMonitors, boolean lockedSynchronizers) {
        this.lockedMonitors = lockedMonitors;
        this.lockedSynchronizers = lockedSynchronizers;
    }

    public boolean isLockedMonitors() {
        return lockedMonitors;
    }

    public boolean isLockedSynchronizers() {
        return lockedSynchronizers;
    }

    /**
     * Resolves a mode name (case-insensitive), returning {@code fallback} when the input is
     * null/blank or does not match a known mode.
     */
    public static ThreadDumpMode fromStringOrDefault(String name, ThreadDumpMode fallback) {
        if (name == null || name.trim().isEmpty()) {
            return fallback;
        }
        for (ThreadDumpMode m : values()) {
            if (m.name().equalsIgnoreCase(name.trim())) {
                return m;
            }
        }
        return fallback;
    }

    /**
     * Returns the effective mode for a REST request: {@code explicit} takes precedence; otherwise
     * {@code clusterDefaultName} (typically the value of {@code cluster.thread-dump.default-mode})
     * is parsed via {@link #fromStringOrDefault(String, ThreadDumpMode)}, falling back to {@link
     * #FULL} for null/blank/unknown values (matching the historical behavior).
     */
    public static ThreadDumpMode resolve(ThreadDumpMode explicit, String clusterDefaultName) {
        return explicit != null ? explicit : fromStringOrDefault(clusterDefaultName, FULL);
    }
}
