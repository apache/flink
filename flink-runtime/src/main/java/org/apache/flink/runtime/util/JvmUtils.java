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

package org.apache.flink.runtime.util;

import org.apache.flink.runtime.messages.ThreadInfoSample;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Utilities for {@link java.lang.management.ManagementFactory}. */
public final class JvmUtils {

    private static final Logger LOG = LoggerFactory.getLogger(JvmUtils.class);

    /**
     * Creates a thread dump of the current JVM.
     *
     * @return the thread dump of current JVM
     */
    public static Collection<ThreadInfo> createThreadDump() {
        ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();

        return Arrays.asList(threadMxBean.dumpAllThreads(true, true));
    }

    /**
     * Creates a {@link ThreadInfoSample} for a specific thread. Contains thread traces if
     * maxStackTraceDepth > 0.
     *
     * @param threadId The ID of the thread to create the thread dump for.
     * @param maxStackTraceDepth The maximum number of entries in the stack trace to be collected.
     * @return The thread information of a specific thread.
     */
    public static Optional<ThreadInfoSample> createThreadInfoSample(
            long threadId, int maxStackTraceDepth) {
        ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();

        return ThreadInfoSample.from(threadMxBean.getThreadInfo(threadId, maxStackTraceDepth));
    }

    /**
     * Creates a {@link ThreadInfoSample} for a specific thread. Contains thread traces if
     * maxStackTraceDepth > 0.
     *
     * @param threadIds The IDs of the threads to create the thread dump for.
     * @param maxStackTraceDepth The maximum number of entries in the stack trace to be collected.
     * @return The map key is the thread id, the map value is the thread information for the
     *     requested thread IDs.
     */
    public static Map<Long, ThreadInfoSample> createThreadInfoSample(
            Collection<Long> threadIds, int maxStackTraceDepth) {
        ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
        long[] threadIdsArray = threadIds.stream().mapToLong(l -> l).toArray();

        ThreadInfo[] threadInfo = threadMxBean.getThreadInfo(threadIdsArray, maxStackTraceDepth);

        List<ThreadInfo> threadInfoNoNulls =
                IntStream.range(0, threadIdsArray.length)
                        .filter(
                                i -> {
                                    if (threadInfo[i] == null) {
                                        LOG.debug(
                                                "FlameGraphs: thread {} is not alive or does not exist.",
                                                threadIdsArray[i]);
                                        return false;
                                    }
                                    return true;
                                })
                        .mapToObj(i -> threadInfo[i])
                        .collect(Collectors.toList());

        return ThreadInfoSample.from(threadInfoNoNulls);
    }

    /**
     * Custom stringify format of JVM thread info to bypass the MAX_FRAMES = 8 limitation.
     *
     * <p>This method is based on
     * https://github.com/openjdk/jdk/blob/master/src/java.management/share/classes/java/lang/management/ThreadInfo.java#L597
     */
    public static String stringifyThreadInfo(
            java.lang.management.ThreadInfo threadInfo, int maxDepth) {
        StringBuilder sb =
                new StringBuilder(
                        "\""
                                + threadInfo.getThreadName()
                                + "\""
                                + " Id="
                                + threadInfo.getThreadId()
                                + " "
                                + threadInfo.getThreadState());
        if (threadInfo.getLockName() != null) {
            sb.append(" on " + threadInfo.getLockName());
        }
        if (threadInfo.getLockOwnerName() != null) {
            sb.append(
                    " owned by \""
                            + threadInfo.getLockOwnerName()
                            + "\" Id="
                            + threadInfo.getLockOwnerId());
        }
        if (threadInfo.isSuspended()) {
            sb.append(" (suspended)");
        }
        if (threadInfo.isInNative()) {
            sb.append(" (in native)");
        }
        sb.append('\n');
        int i = 0;
        StackTraceElement[] stackTraceElements = threadInfo.getStackTrace();
        for (; i < stackTraceElements.length && i < maxDepth; i++) {
            StackTraceElement ste = stackTraceElements[i];
            sb.append("\tat " + ste.toString());
            sb.append('\n');
            if (i == 0 && threadInfo.getLockInfo() != null) {
                Thread.State ts = threadInfo.getThreadState();
                switch (ts) {
                    case BLOCKED:
                        sb.append("\t-  blocked on " + threadInfo.getLockInfo());
                        sb.append('\n');
                        break;
                    case WAITING:
                    case TIMED_WAITING:
                        sb.append("\t-  waiting on " + threadInfo.getLockInfo());
                        sb.append('\n');
                        break;
                    default:
                }
            }

            for (MonitorInfo mi : threadInfo.getLockedMonitors()) {
                if (mi.getLockedStackDepth() == i) {
                    sb.append("\t-  locked " + mi);
                    sb.append('\n');
                }
            }
        }
        if (i < threadInfo.getStackTrace().length) {
            sb.append("\t...");
            sb.append('\n');
        }

        LockInfo[] locks = threadInfo.getLockedSynchronizers();
        if (locks.length > 0) {
            sb.append("\n\tNumber of locked synchronizers = " + locks.length);
            sb.append('\n');
            for (LockInfo li : locks) {
                sb.append("\t- " + li);
                sb.append('\n');
            }
        }
        sb.append('\n');
        return sb.toString();
    }

    /** Private default constructor to avoid instantiation. */
    private JvmUtils() {}
}
