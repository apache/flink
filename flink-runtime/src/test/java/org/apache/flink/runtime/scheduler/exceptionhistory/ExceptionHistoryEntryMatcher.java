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
import org.apache.flink.util.ExceptionUtils;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

/** Matches {@link ExceptionHistoryEntry} instances. */
public class ExceptionHistoryEntryMatcher extends TypeSafeDiagnosingMatcher<ExceptionHistoryEntry> {

    public static Matcher<ExceptionHistoryEntry> matchesGlobalFailure(
            Throwable expectedException, long expectedTimestamp) {
        return matchesFailure(expectedException, expectedTimestamp, null, null);
    }

    public static Matcher<ExceptionHistoryEntry> matchesFailure(
            Throwable expectedException,
            long expectedTimestamp,
            String expectedTaskName,
            TaskManagerLocation expectedTaskManagerLocation) {
        return new ExceptionHistoryEntryMatcher(
                expectedException,
                expectedTimestamp,
                expectedTaskName,
                expectedTaskManagerLocation);
    }

    private final Throwable expectedException;
    private final long expectedTimestamp;
    private final String expectedTaskName;
    private final ArchivedTaskManagerLocationMatcher taskManagerLocationMatcher;

    public ExceptionHistoryEntryMatcher(
            Throwable expectedException,
            long expectedTimestamp,
            String expectedTaskName,
            TaskManagerLocation expectedTaskManagerLocation) {
        this.expectedException = expectedException;
        this.expectedTimestamp = expectedTimestamp;
        this.expectedTaskName = expectedTaskName;
        this.taskManagerLocationMatcher =
                new ArchivedTaskManagerLocationMatcher(expectedTaskManagerLocation);
    }

    @Override
    protected boolean matchesSafely(
            ExceptionHistoryEntry exceptionHistoryEntry, Description description) {
        boolean match = true;
        if (!exceptionHistoryEntry
                .getException()
                .deserializeError(ClassLoader.getSystemClassLoader())
                .equals(expectedException)) {
            description
                    .appendText(" actualException=")
                    .appendText(
                            ExceptionUtils.stringifyException(
                                    exceptionHistoryEntry
                                            .getException()
                                            .deserializeError(ClassLoader.getSystemClassLoader())));
            match = false;
        }

        if (exceptionHistoryEntry.getTimestamp() != expectedTimestamp) {
            description
                    .appendText(" actualTimestamp=")
                    .appendText(String.valueOf(exceptionHistoryEntry.getTimestamp()));
            match = false;
        }

        if (exceptionHistoryEntry.getFailingTaskName() == null) {
            if (expectedTaskName != null) {
                description.appendText(" actualTaskName=null");
                match = false;
            }
        } else if (exceptionHistoryEntry.getFailingTaskName().equals(expectedTaskName)) {
            description
                    .appendText(" actualTaskName=")
                    .appendText(exceptionHistoryEntry.getFailingTaskName());
            match = false;
        }

        match |=
                taskManagerLocationMatcher.matchesSafely(
                        exceptionHistoryEntry.getTaskManagerLocation(), description);

        return match;
    }

    @Override
    public void describeTo(Description description) {
        description
                .appendText("expectedException=")
                .appendText(ExceptionUtils.stringifyException(expectedException))
                .appendText(" expectedTimestamp=")
                .appendText(String.valueOf(expectedTimestamp))
                .appendText(" expectedTaskName=")
                .appendText(expectedTaskName)
                .appendText(" expectedTaskManagerLocation=");
        taskManagerLocationMatcher.describeTo(description);
    }
}
