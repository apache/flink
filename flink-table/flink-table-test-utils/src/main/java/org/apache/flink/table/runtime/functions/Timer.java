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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.utils.DateTimeUtils;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Objects;

/**
 * A timer registered by a {@link org.apache.flink.table.functions.ProcessTableFunction} during
 * testing.
 */
@PublicEvolving
public class Timer implements Comparable<Timer> {

    final long timestamp;
    @Nullable final String name;
    final Row partitionKey;
    private boolean fired;

    Timer(long timestamp, @Nullable String name, Row partitionKey) {
        this.timestamp = timestamp;
        this.name = name;
        this.partitionKey = partitionKey;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @SuppressWarnings("unchecked")
    public <T> T getTimestampAs(Class<T> timeClass) {
        if (timeClass == Long.class || timeClass == long.class) {
            return (T) Long.valueOf(timestamp);
        } else if (timeClass == Instant.class) {
            return (T) Instant.ofEpochMilli(timestamp);
        } else if (timeClass == LocalDateTime.class) {
            return (T) DateTimeUtils.toLocalDateTime(timestamp);
        } else if (timeClass == java.sql.Timestamp.class) {
            return (T) DateTimeUtils.toSQLTimestamp(timestamp);
        }
        throw new IllegalArgumentException("Unsupported time type: " + timeClass);
    }

    @Nullable
    public String getName() {
        return name;
    }

    public Row getKey() {
        return partitionKey;
    }

    public boolean hasFired() {
        return fired;
    }

    void markFired() {
        this.fired = true;
    }

    /**
     * Comparison of timers is done by timestamp first, then by name (unnamed timers sort after
     * named ones), then by partition key, for a deterministic firing order.
     */
    @Override
    public int compareTo(Timer other) {
        int cmp = Long.compare(this.timestamp, other.timestamp);
        if (cmp != 0) {
            return cmp;
        }
        if (this.name == null && other.name == null) {
            return this.partitionKey.toString().compareTo(other.partitionKey.toString());
        }
        if (this.name == null) {
            return 1;
        }
        if (other.name == null) {
            return -1;
        }
        cmp = this.name.compareTo(other.name);
        if (cmp != 0) {
            return cmp;
        }
        return this.partitionKey.toString().compareTo(other.partitionKey.toString());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Timer)) {
            return false;
        }
        Timer that = (Timer) o;
        return this.timestamp == that.timestamp
                && Objects.equals(this.name, that.name)
                && Objects.equals(this.partitionKey, that.partitionKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, name, partitionKey);
    }

    @Override
    public String toString() {
        return String.format(
                "Timer{timestamp=%d, name=%s, key=%s, fired=%s}",
                timestamp, name != null ? "'" + name + "'" : "null", partitionKey, fired);
    }
}
