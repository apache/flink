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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.util.JvmUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.lang.management.LockInfo;
import java.lang.management.MonitorInfo;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

/** Class containing thread dump information. */
public final class ThreadDumpInfo implements ResponseBody, Serializable {
    private static final long serialVersionUID = 1L;

    public static final String FIELD_NAME_THREAD_INFOS = "threadInfos";

    @JsonProperty(FIELD_NAME_THREAD_INFOS)
    private final Collection<ThreadInfo> threadInfos;

    private ThreadDumpInfo(Collection<ThreadInfo> threadInfos) {
        this.threadInfos = threadInfos;
    }

    public Collection<ThreadInfo> getThreadInfos() {
        return threadInfos;
    }

    @JsonCreator
    public static ThreadDumpInfo create(
            @JsonProperty(FIELD_NAME_THREAD_INFOS) Collection<ThreadInfo> threadInfos) {
        return new ThreadDumpInfo(threadInfos);
    }

    public static ThreadDumpInfo dumpAndCreate(int stacktraceMaxDepth) {
        return create(
                JvmUtils.createThreadDump().stream()
                        .map(
                                threadInfo ->
                                        ThreadDumpInfo.ThreadInfo.create(
                                                threadInfo.getThreadName(),
                                                stringifyThreadInfo(
                                                        threadInfo, stacktraceMaxDepth)))
                        .collect(Collectors.toList()));
    }

    /**
     * Custom stringify format of JVM thread info to bypass the MAX_FRAMES = 8 limitation.
     *
     * <p>This method is based on
     * https://github.com/openjdk/jdk/blob/master/src/java.management/share/classes/java/lang/management/ThreadInfo.java#L597
     */
    @VisibleForTesting
    protected static String stringifyThreadInfo(
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

    /** Class containing information about a thread. */
    public static final class ThreadInfo implements Serializable {
        private static final long serialVersionUID = 1L;

        public static final String FIELD_NAME_THREAD_NAME = "threadName";

        public static final String FIELD_NAME_THREAD_INFO = "stringifiedThreadInfo";

        @JsonProperty(FIELD_NAME_THREAD_NAME)
        private final String threadName;

        @JsonProperty(FIELD_NAME_THREAD_INFO)
        private final String stringifiedThreadInfo;

        private ThreadInfo(String threadName, String stringifiedThreadInfo) {
            this.threadName = threadName;
            this.stringifiedThreadInfo = stringifiedThreadInfo;
        }

        @JsonCreator
        public static ThreadInfo create(
                @JsonProperty(FIELD_NAME_THREAD_NAME) String threadName,
                @JsonProperty(FIELD_NAME_THREAD_INFO) String stringifiedThreadInfo) {
            return new ThreadInfo(threadName, stringifiedThreadInfo);
        }

        public String getThreadName() {
            return threadName;
        }

        public String getStringifiedThreadInfo() {
            return stringifiedThreadInfo;
        }

        @Override
        public String toString() {
            return stringifiedThreadInfo;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ThreadInfo that = (ThreadInfo) o;
            return Objects.equals(threadName, that.threadName)
                    && Objects.equals(stringifiedThreadInfo, that.stringifiedThreadInfo);
        }

        @Override
        public int hashCode() {
            return Objects.hash(threadName, stringifiedThreadInfo);
        }
    }
}
