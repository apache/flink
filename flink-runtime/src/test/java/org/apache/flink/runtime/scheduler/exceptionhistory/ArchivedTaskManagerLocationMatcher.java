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

import org.apache.flink.runtime.scheduler.exceptionhistory.ExceptionHistoryEntry.ArchivedTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.Objects;
import java.util.function.Predicate;

import static org.apache.flink.runtime.scheduler.exceptionhistory.ExceptionHistoryEntry.ArchivedTaskManagerLocation.fromTaskManagerLocation;

/**
 * {@code ArchivedTaskManagerLocationMatcher} can be used to match {@link TaskManagerLocation} with
 * {@link ArchivedTaskManagerLocation} instances.
 */
class ArchivedTaskManagerLocationMatcher implements Predicate<ArchivedTaskManagerLocation> {

    private final ArchivedTaskManagerLocation expectedLocation;

    public static Predicate<ArchivedTaskManagerLocation> isArchivedTaskManagerLocation(
            TaskManagerLocation actualLocation) {
        return new ArchivedTaskManagerLocationMatcher(actualLocation);
    }

    ArchivedTaskManagerLocationMatcher(TaskManagerLocation expectedLocation) {
        this(fromTaskManagerLocation(expectedLocation));
    }

    ArchivedTaskManagerLocationMatcher(ArchivedTaskManagerLocation expectedLocation) {
        this.expectedLocation = expectedLocation;
    }

    @Override
    public boolean test(ArchivedTaskManagerLocation actual) {
        if (actual == null) {
            return expectedLocation == null;
        } else if (expectedLocation == null) {
            return false;
        }

        boolean match = true;
        if (!Objects.equals(actual.getAddress(), expectedLocation.getAddress())) {
            match = false;
        }

        if (!Objects.equals(actual.getFQDNHostname(), expectedLocation.getFQDNHostname())) {
            match = false;
        }

        if (!Objects.equals(actual.getHostname(), expectedLocation.getHostname())) {
            match = false;
        }

        if (!Objects.equals(actual.getResourceID(), expectedLocation.getResourceID())) {
            match = false;
        }

        if (!Objects.equals(actual.getPort(), expectedLocation.getPort())) {
            match = false;
        }

        return match;
    }
}
