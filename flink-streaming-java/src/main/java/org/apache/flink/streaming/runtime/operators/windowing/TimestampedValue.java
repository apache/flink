/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Stores the value and the timestamp of the record.
 *
 * @param <T> The type encapsulated value
 */
@PublicEvolving
public class TimestampedValue<T> {

    /** The actual value held by this record. */
    private T value;

    /** The timestamp of the record. */
    private long timestamp;

    /** Flag whether the timestamp is actually set. */
    private boolean hasTimestamp;

    /** Creates a new TimestampedValue. The record does not have a timestamp. */
    public TimestampedValue(T value) {
        this.value = value;
    }

    /**
     * Creates a new TimestampedValue wrapping the given value. The timestamp is set to the given
     * timestamp.
     *
     * @param value The value to wrap in this {@link TimestampedValue}
     * @param timestamp The timestamp in milliseconds
     */
    public TimestampedValue(T value, long timestamp) {
        this.value = value;
        this.timestamp = timestamp;
        this.hasTimestamp = true;
    }

    /** @return The value wrapped in this {@link TimestampedValue}. */
    public T getValue() {
        return value;
    }

    /** @return The timestamp associated with this stream value in milliseconds. */
    public long getTimestamp() {
        if (hasTimestamp) {
            return timestamp;
        } else {
            throw new IllegalStateException(
                    "Record has no timestamp. Is the time characteristic set to 'ProcessingTime', or "
                            + "did you forget to call 'DataStream.assignTimestampsAndWatermarks(...)'?");
        }
    }

    /**
     * Checks whether this record has a timestamp.
     *
     * @return True if the record has a timestamp, false if not.
     */
    public boolean hasTimestamp() {
        return hasTimestamp;
    }

    /** Creates a {@link StreamRecord} from this TimestampedValue. */
    public StreamRecord<T> getStreamRecord() {
        StreamRecord<T> streamRecord = new StreamRecord<>(value);
        if (hasTimestamp) {
            streamRecord.setTimestamp(timestamp);
        }
        return streamRecord;
    }

    /**
     * Creates a TimestampedValue from given {@link StreamRecord}.
     *
     * @param streamRecord The StreamRecord object from which TimestampedValue is to be created.
     */
    public static <T> TimestampedValue<T> from(StreamRecord<T> streamRecord) {
        if (streamRecord.hasTimestamp()) {
            return new TimestampedValue<>(streamRecord.getValue(), streamRecord.getTimestamp());
        } else {
            return new TimestampedValue<>(streamRecord.getValue());
        }
    }
}
