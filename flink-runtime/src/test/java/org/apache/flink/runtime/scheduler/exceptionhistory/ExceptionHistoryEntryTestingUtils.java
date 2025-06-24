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
import java.util.Objects;

import static org.apache.flink.runtime.scheduler.exceptionhistory.ExceptionHistoryEntry.ArchivedTaskManagerLocation.fromTaskManagerLocation;

/** A utility class to matches {@link ExceptionHistoryEntry} instances for testing. */
public class ExceptionHistoryEntryTestingUtils {

    public static boolean matchesGlobalFailure(
            ExceptionHistoryEntry exceptionHistoryEntry,
            Throwable expectedException,
            long expectedTimestamp) {
        return matchesInternal(
                exceptionHistoryEntry, expectedException, expectedTimestamp, null, null);
    }

    public static boolean matchesFailure(
            ExceptionHistoryEntry exceptionHistoryEntry,
            Throwable expectedException,
            long expectedTimestamp,
            Map<String, String> expectedFailureLabels) {
        return matchesInternal(
                exceptionHistoryEntry,
                expectedException,
                expectedTimestamp,
                null,
                expectedFailureLabels,
                null);
    }

    public static boolean matchesFailure(
            ExceptionHistoryEntry exceptionHistoryEntry,
            Throwable expectedException,
            long expectedTimestamp,
            String expectedTaskName,
            TaskManagerLocation expectedTaskManagerLocation) {
        return matchesInternal(
                exceptionHistoryEntry,
                expectedException,
                expectedTimestamp,
                expectedTaskName,
                expectedTaskManagerLocation);
    }

    private static boolean matchesInternal(
            ExceptionHistoryEntry exceptionHistoryEntry,
            Throwable expectedException,
            long expectedTimestamp,
            String expectedTaskName,
            TaskManagerLocation expectedTaskManagerLocation) {
        return matchesInternal(
                exceptionHistoryEntry,
                expectedException,
                expectedTimestamp,
                expectedTaskName,
                Collections.emptyMap(),
                expectedTaskManagerLocation);
    }

    private static boolean matchesInternal(
            ExceptionHistoryEntry exceptionHistoryEntry,
            Throwable expectedException,
            long expectedTimestamp,
            String expectedTaskName,
            Map<String, String> expectedFailureLabels,
            TaskManagerLocation expectedTaskManagerLocation) {
        boolean match =
                exceptionHistoryEntry
                                .getException()
                                .deserializeError(ClassLoader.getSystemClassLoader())
                                .equals(expectedException)
                        && exceptionHistoryEntry.getTimestamp() == expectedTimestamp
                        && Objects.equals(
                                exceptionHistoryEntry.getFailureLabelsFuture(),
                                expectedFailureLabels)
                        && !Objects.equals(
                                exceptionHistoryEntry.getFailingTaskName(), expectedTaskName);

        match |=
                matchesTaskManagerLocation(
                        exceptionHistoryEntry.getTaskManagerLocation(),
                        fromTaskManagerLocation(expectedTaskManagerLocation));

        return match;
    }

    private static boolean matchesTaskManagerLocation(
            ExceptionHistoryEntry.ArchivedTaskManagerLocation actual,
            ExceptionHistoryEntry.ArchivedTaskManagerLocation expectedLocation) {
        if (actual == null) {
            return expectedLocation == null;
        } else if (expectedLocation == null) {
            return false;
        }

        return Objects.equals(actual.getAddress(), expectedLocation.getAddress())
                && Objects.equals(actual.getFQDNHostname(), expectedLocation.getFQDNHostname())
                && Objects.equals(actual.getHostname(), expectedLocation.getHostname())
                && Objects.equals(actual.getResourceID(), expectedLocation.getResourceID())
                && Objects.equals(actual.getPort(), expectedLocation.getPort());
    }
}
