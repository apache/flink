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

package org.apache.flink.runtime.scheduler.exceptionhistory;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@code RootExceptionHistoryEntry} extending {@link ExceptionHistoryEntry} by providing a list of
 * {@code ExceptionHistoryEntry} instances to store concurrently caught failures.
 */
public class RootExceptionHistoryEntry extends ExceptionHistoryEntry {

    private static final long serialVersionUID = -7647332765867297434L;

    private final Collection<ExceptionHistoryEntry> concurrentExceptions = new ArrayList<>();

    /**
     * Creates a {@code RootExceptionHistoryEntry} representing a global failure from the passed
     * {@code Throwable} and timestamp.
     *
     * @param cause The reason for the failure.
     * @param timestamp The time the failure was caught.
     * @return The {@code RootExceptionHistoryEntry} instance.
     * @throws NullPointerException if {@code cause} is {@code null}.
     * @throws IllegalArgumentException if the passed {@code timestamp} is not bigger than {@code
     *     0}.
     */
    @VisibleForTesting
    public static RootExceptionHistoryEntry fromGlobalFailure(Throwable cause, long timestamp) {
        return new RootExceptionHistoryEntry(cause, timestamp, null, null);
    }

    /**
     * Creates a {@code RootExceptionHistoryEntry} representing a local failure using the passed
     * information.
     *
     * @param cause The reason for the failure.
     * @param timestamp The time the failure was caught.
     * @param failingTaskName The name of the task that failed.
     * @param taskManagerLocation The {@link TaskManagerLocation} the task was running on.
     * @return The {@code RootExceptionHistoryEntry} instance.
     * @throws NullPointerException if {@code cause} or {@code failingTaskName} are {@code null}.
     * @throws IllegalArgumentException if the passed {@code timestamp} is not bigger than {@code
     *     0}.
     */
    @VisibleForTesting
    public static RootExceptionHistoryEntry fromLocalFailure(
            Throwable cause,
            long timestamp,
            String failingTaskName,
            @Nullable TaskManagerLocation taskManagerLocation) {
        return new RootExceptionHistoryEntry(
                cause, timestamp, checkNotNull(failingTaskName), taskManagerLocation);
    }

    private RootExceptionHistoryEntry(
            Throwable cause,
            long timestamp,
            @Nullable String failingTaskName,
            @Nullable TaskManagerLocation taskManagerLocation) {
        super(cause, timestamp, failingTaskName, taskManagerLocation);
    }

    void add(ExceptionHistoryEntry concurrentException) {
        concurrentExceptions.add(concurrentException);
    }

    public Iterable<ExceptionHistoryEntry> getConcurrentExceptions() {
        return concurrentExceptions;
    }
}
