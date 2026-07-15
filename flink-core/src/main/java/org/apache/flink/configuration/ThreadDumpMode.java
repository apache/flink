/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.description.InlineElement;

import static org.apache.flink.configuration.description.TextElement.text;

/**
 * Granularity of the thread dump collected via {@link
 * java.lang.management.ThreadMXBean#dumpAllThreads(boolean, boolean)}. Information about the lock
 * each thread is currently waiting on ({@link java.lang.management.ThreadInfo#getLockInfo()}) is
 * populated in both modes.
 *
 * @see ClusterOptions#THREAD_DUMP_DEFAULT_MODE
 */
@PublicEvolving
public enum ThreadDumpMode implements DescribedEnum {

    /**
     * {@code dumpAllThreads(false, false)}: stack traces only, no lock info (jstack without {@code
     * -l}). Negligible JVM pause.
     */
    LITE(false, false, text("Stack traces only, without lock information. Negligible JVM pause.")),

    /**
     * {@code dumpAllThreads(true, true)}: also collects locked monitors and j.u.c. synchronizers
     * (equivalent to {@code jstack -l}). Pauses the JVM in a safepoint for a duration that scales
     * with heap size and thread count -- seconds on large TaskManagers.
     */
    FULL(
            true,
            true,
            text(
                    "Additionally collects locked monitors and j.u.c. synchronizers, equivalent to jstack -l. "
                            + "Pauses the JVM in a safepoint for a duration that scales with heap size and "
                            + "thread count, which can take seconds on large TaskManagers."));

    private final boolean lockedMonitors;
    private final boolean lockedSynchronizers;
    private final InlineElement description;

    ThreadDumpMode(boolean lockedMonitors, boolean lockedSynchronizers, InlineElement description) {
        this.lockedMonitors = lockedMonitors;
        this.lockedSynchronizers = lockedSynchronizers;
        this.description = description;
    }

    public boolean isLockedMonitors() {
        return lockedMonitors;
    }

    public boolean isLockedSynchronizers() {
        return lockedSynchronizers;
    }

    @Override
    public InlineElement getDescription() {
        return description;
    }
}
