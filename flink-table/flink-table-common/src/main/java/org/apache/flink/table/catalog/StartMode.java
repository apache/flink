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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.Objects;

/** The start mode of materialized table. */
@PublicEvolving
public class StartMode {
    private final StartModeKind kind;
    private final @Nullable Instant timestamp;
    private final boolean localTimeZone;
    private final @Nullable Interval interval;

    @PublicEvolving
    public enum StartModeKind {
        FROM_BEGINNING,
        FROM_NOW,
        FROM_TIMESTAMP,
        RESUME_OR_FROM_BEGINNING,
        RESUME_OR_FROM_NOW,
        RESUME_OR_FROM_TIMESTAMP;
    }

    private StartMode(
            StartModeKind kind,
            @Nullable Instant timestamp,
            boolean localTimeZone,
            @Nullable Interval interval) {
        this.kind = kind;
        this.timestamp = timestamp;
        this.localTimeZone = localTimeZone;
        this.interval = interval;
    }

    public static StartMode of(StartModeKind kind) {
        return new StartMode(kind, null, false, null);
    }

    public static StartMode of(StartModeKind kind, Instant timestamp) {
        return new StartMode(kind, timestamp, false, null);
    }

    public static StartMode of(StartModeKind kind, Instant timestamp, boolean localTimeZone) {
        return new StartMode(kind, timestamp, localTimeZone, null);
    }

    public static StartMode of(StartModeKind kind, Interval interval) {
        return new StartMode(kind, null, false, interval);
    }

    public static boolean requiresParameters(StartModeKind kind) {
        return kind == StartModeKind.FROM_TIMESTAMP
                || kind == StartModeKind.RESUME_OR_FROM_TIMESTAMP;
    }

    public StartModeKind getKind() {
        return kind;
    }

    @Nullable
    public Instant getTimestamp() {
        return timestamp;
    }

    public boolean isLocalTimeZone() {
        return localTimeZone;
    }

    @Nullable
    public Interval getInterval() {
        return interval;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StartMode startMode = (StartMode) o;
        return localTimeZone == startMode.localTimeZone
                && kind == startMode.kind
                && Objects.equals(timestamp, startMode.timestamp)
                && Objects.equals(interval, startMode.interval);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, timestamp, localTimeZone, interval);
    }

    public String asSummaryString() {
        switch (kind) {
            case FROM_BEGINNING:
            case RESUME_OR_FROM_BEGINNING:
                return kind.name();
            case FROM_NOW:
            case RESUME_OR_FROM_NOW:
                if (interval == null) {
                    return kind.name();
                }

                return kind.name() + "(" + interval + ")";

            case FROM_TIMESTAMP:
            case RESUME_OR_FROM_TIMESTAMP:
                return kind.name()
                        + "(TIMESTAMP "
                        + (localTimeZone ? "WITH LOCAL TIME ZONE " : "")
                        + "'"
                        + timestamp
                        + "')";

            default:
                throw new IllegalStateException("Unexpected StartModeKind: " + kind);
        }
    }

    @Override
    public String toString() {
        return asSummaryString();
    }
}
