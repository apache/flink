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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions.SystemOutMode;

import org.apache.commons.io.output.NullPrintStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Optional;

import static org.apache.flink.configuration.TaskManagerOptions.TASK_MANAGER_SYSTEM_OUT_LOG_CACHE_SIZE;
import static org.apache.flink.configuration.TaskManagerOptions.TASK_MANAGER_SYSTEM_OUT_LOG_THREAD_NAME;
import static org.apache.flink.configuration.TaskManagerOptions.TASK_MANAGER_SYSTEM_OUT_MODE;

/** Utility class for redirect the {@link System#out} and {@link System#err}. */
public class SystemOutRedirectionUtils {

    private static final Logger LOG = LoggerFactory.getLogger(SystemOutRedirectionUtils.class);

    /**
     * Redirect {@link System#out} and {@link System#err} based on {@link
     * TaskManagerOptions#TASK_MANAGER_SYSTEM_OUT_MODE} related options.
     */
    public static void redirectSystemOutAndError(Configuration conf) {
        SystemOutMode systemOutMode = conf.get(TASK_MANAGER_SYSTEM_OUT_MODE);
        switch (systemOutMode) {
            case LOG:
                redirectToCurrentLog(
                        conf.get(TASK_MANAGER_SYSTEM_OUT_LOG_CACHE_SIZE).getBytes(),
                        conf.get(TASK_MANAGER_SYSTEM_OUT_LOG_THREAD_NAME));
                break;
            case IGNORE:
                ignoreSystemOutAndError();
                break;
            case DEFAULT:
            default:
                break;
        }
    }

    private static void ignoreSystemOutAndError() {
        System.setOut(new NullPrintStream());
        System.setErr(new NullPrintStream());
    }

    private static void redirectToCurrentLog(long byteLimitEachLine, boolean logThreadName) {
        redirectToLoggingRedirector(LOG::info, LOG::error, byteLimitEachLine, logThreadName);
    }

    @VisibleForTesting
    static void redirectToLoggingRedirector(
            LoggingRedirector outRedirector,
            LoggingRedirector errRedirector,
            long byteLimitEachLine,
            boolean logThreadName) {
        System.setOut(new LoggingPrintStream(outRedirector, byteLimitEachLine, logThreadName));
        System.setErr(new LoggingPrintStream(errRedirector, byteLimitEachLine, logThreadName));
    }

    /** The redirector of log. */
    @VisibleForTesting
    interface LoggingRedirector {

        void redirect(String logContext);
    }

    /** Redirect the PrintStream to LoggingRedirector. */
    private static class LoggingPrintStream extends PrintStream {

        private final LoggingRedirector loggingRedirector;

        private final LineContextCache helper;

        private final boolean logThreadName;

        private LoggingPrintStream(
                LoggingRedirector loggingRedirector,
                long byteLimitEachLine,
                boolean logThreadName) {
            super(new LineContextCache(byteLimitEachLine));
            helper = (LineContextCache) super.out;
            this.loggingRedirector = loggingRedirector;
            this.logThreadName = logThreadName;
        }

        public void write(int b) {
            super.write(b);
            tryLogCurrentLine();
        }

        public void write(byte[] buf, int off, int len) {
            super.write(buf, off, len);
            tryLogCurrentLine();
        }

        private void tryLogCurrentLine() {
            synchronized (this) {
                helper.tryGenerateContext()
                        .ifPresent(
                                logContext -> {
                                    if (!logThreadName) {
                                        loggingRedirector.redirect(logContext);
                                        return;
                                    }
                                    loggingRedirector.redirect(
                                            String.format(
                                                    "Thread Name: %s , log context: %s",
                                                    Thread.currentThread().getName(), logContext));
                                });
            }
        }
    }

    /**
     * Cache the context of current line. When current line is ended or the context size reaches the
     * upper size, it can generate the line context.
     */
    private static class LineContextCache extends ByteArrayOutputStream {

        private static final byte[] LINE_SEPARATOR_BYTES = System.lineSeparator().getBytes();
        private static final int LINE_SEPARATOR_LENGTH = LINE_SEPARATOR_BYTES.length;

        /** The upper byte size of current line. */
        private final long byteLimitEachLine;

        private LineContextCache(long byteLimitEachLine) {
            this.byteLimitEachLine = byteLimitEachLine;
        }

        public synchronized Optional<String> tryGenerateContext() {
            if (isLineEnded()) {
                try {
                    return Optional.of(new String(buf, 0, count - LINE_SEPARATOR_LENGTH));
                } finally {
                    reset();
                }
            }
            if (count >= byteLimitEachLine) {
                try {
                    return Optional.of(new String(buf, 0, count));
                } finally {
                    reset();
                }
            }
            return Optional.empty();
        }

        private synchronized boolean isLineEnded() {
            if (count < LINE_SEPARATOR_LENGTH) {
                return false;
            }

            if (LINE_SEPARATOR_LENGTH == 1) {
                return LINE_SEPARATOR_BYTES[0] == buf[count - 1];
            }

            for (int i = 0; i < LINE_SEPARATOR_LENGTH; i++) {
                if (LINE_SEPARATOR_BYTES[i] == buf[count - LINE_SEPARATOR_LENGTH + i]) {
                    continue;
                }
                return false;
            }
            return true;
        }
    }
}
