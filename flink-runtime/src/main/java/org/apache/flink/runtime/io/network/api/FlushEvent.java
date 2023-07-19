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

package org.apache.flink.runtime.io.network.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.RuntimeEvent;

import java.io.IOException;

/**
 * On receiving a flush event, an operator should trigger a flush operation, and broadcast flush
 * event to all down-streaming operators.
 */
@PublicEvolving
public class FlushEvent extends RuntimeEvent {

    private final long flushEventId;
    private final long timestamp;

    public FlushEvent(long flushEventId, long timestamp) {
        this.flushEventId = flushEventId;
        this.timestamp = timestamp;
    }

    public long getFlushEventId() {
        return flushEventId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        throw new UnsupportedOperationException("This method should never be called");
    }

    @Override
    public void read(DataInputView in) throws IOException {
        throw new UnsupportedOperationException("This method should never be called");
    }

    // ------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return (int) (flushEventId ^ (flushEventId >>> 32) ^ timestamp ^ (timestamp >>> 32));
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        } else if (other == null || other.getClass() != FlushEvent.class) {
            return false;
        } else {
            FlushEvent that = (FlushEvent) other;
            return that.flushEventId == this.flushEventId && that.timestamp == this.timestamp;
        }
    }

    @Override
    public String toString() {
        return String.format("Flush Event %d @ %d", flushEventId, timestamp);
    }
}
