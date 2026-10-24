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

package org.apache.flink.table.runtime.operators.join.temporal;

import org.apache.flink.annotation.Internal;

/**
 * Probe-side state key of {@link TemporalRowTimeJoinOperatorV2}: the row time of the probe record
 * first, then a per-key arrival index to keep records with the same row time distinct and to
 * restore arrival order at emission time.
 */
@Internal
public final class LeftTimeIndexKey {

    private final long timestamp;
    private final long index;

    public LeftTimeIndexKey(long timestamp, long index) {
        this.timestamp = timestamp;
        this.index = index;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getIndex() {
        return index;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LeftTimeIndexKey that = (LeftTimeIndexKey) o;
        return timestamp == that.timestamp && index == that.index;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(timestamp);
        return 31 * result + Long.hashCode(index);
    }

    @Override
    public String toString() {
        return "LeftTimeIndexKey{timestamp=" + timestamp + ", index=" + index + '}';
    }
}
