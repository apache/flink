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

import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.Collections;
import java.util.Map;
import java.util.function.Predicate;

/** Matches {@link ExceptionHistoryEntry} instances. */
public class ExceptionHistoryEntryMatcher implements Predicate<ExceptionHistoryEntry> {

    public static Predicate<ExceptionHistoryEntry> matchesGlobalFailure(
            Throwable expectedException,
            long expectedTimestamp,
            Map<String, String> expectedFailureLabels) {
        return matchesFailure(
                expectedException, expectedTimestamp, expectedFailureLabels, null, null);
    }

    public static Predicate<ExceptionHistoryEntry> matchesFailure(
            Throwable expectedException,
            long expectedTimestamp,
            String expectedTaskName,
            TaskManagerLocation expectedTaskManagerLocation) {
        return new ExceptionHistoryEntryMatcher(
                expectedException,
                expectedTimestamp,
                Collections.emptyMap(),
                expectedTaskName,
                expectedTaskManagerLocation);
    }

    public static Predicate<ExceptionHistoryEntry> matchesFailure(
            Throwable expectedException,
            long expectedTimestamp,
            Map<String, String> expectedFailureLabels,
            String expectedTaskName,
            TaskManagerLocation expectedTaskManagerLocation) {
        return new ExceptionHistoryEntryMatcher(
                expectedException,
                expectedTimestamp,
                expectedFailureLabels,
                expectedTaskName,
                expectedTaskManagerLocation);
    }

    private final Throwable expectedException;
    private final long expectedTimestamp;
    private final Map<String, String> expectedFailureLabels;
    private final String expectedTaskName;
    private final ArchivedTaskManagerLocationMatcher taskManagerLocationMatcher;

    public ExceptionHistoryEntryMatcher(
            Throwable expectedException,
            long expectedTimestamp,
            Map<String, String> expectedFailureLabels,
            String expectedTaskName,
            TaskManagerLocation expectedTaskManagerLocation) {
        this.expectedException = expectedException;
        this.expectedTimestamp = expectedTimestamp;
        this.expectedFailureLabels = expectedFailureLabels;
        this.expectedTaskName = expectedTaskName;
        this.taskManagerLocationMatcher =
                new ArchivedTaskManagerLocationMatcher(expectedTaskManagerLocation);
    }

    @Override
    public boolean test(ExceptionHistoryEntry exceptionHistoryEntry) {
        boolean match = true;
        if (!exceptionHistoryEntry
                .getException()
                .deserializeError(ClassLoader.getSystemClassLoader())
                .equals(expectedException)) {
            match = false;
        }

        if (exceptionHistoryEntry.getTimestamp() != expectedTimestamp) {
            match = false;
        }

        if (!exceptionHistoryEntry.getFailureLabelsFuture().equals(expectedFailureLabels)) {
            match = false;
        }

        if (exceptionHistoryEntry.getFailingTaskName() == null) {
            if (expectedTaskName != null) {
                match = false;
            }
        } else if (exceptionHistoryEntry.getFailingTaskName().equals(expectedTaskName)) {
            match = false;
        }

        match |= taskManagerLocationMatcher.test(exceptionHistoryEntry.getTaskManagerLocation());

        return match;
    }
}
