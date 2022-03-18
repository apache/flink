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

package org.apache.flink.table.runtime.operators.window;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * A {@link Window} that represents a time interval from {@code start} (inclusive) to {@code end}
 * (exclusive).
 */
public class TimeWindow extends Window {

    private final long start;
    private final long end;

    public TimeWindow(long start, long end) {
        this.start = start;
        this.end = end;
    }

    /**
     * Gets the starting timestamp of the window. This is the first timestamp that belongs to this
     * window.
     *
     * @return The starting timestamp of this window.
     */
    public long getStart() {
        return start;
    }

    /**
     * Gets the end timestamp of this window. The end timestamp is exclusive, meaning it is the
     * first timestamp that does not belong to this window any more.
     *
     * @return The exclusive end timestamp of this window.
     */
    public long getEnd() {
        return end;
    }

    /**
     * Gets the largest timestamp that still belongs to this window.
     *
     * <p>This timestamp is identical to {@code getEnd() - 1}.
     *
     * @return The largest timestamp that still belongs to this window.
     * @see #getEnd()
     */
    @Override
    public long maxTimestamp() {
        return end - 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TimeWindow window = (TimeWindow) o;

        return end == window.end && start == window.start;
    }

    @Override
    public int hashCode() {
        // inspired from Apache BEAM
        // The end values are themselves likely to be arithmetic sequence, which
        // is a poor distribution to use for a hashtable, so we
        // add a highly non-linear transformation.
        return (int) (start + modInverse((int) (end << 1) + 1));
    }

    /** Compute the inverse of (odd) x mod 2^32. */
    private int modInverse(int x) {
        // Cube gives inverse mod 2^4, as x^4 == 1 (mod 2^4) for all odd x.
        int inverse = x * x * x;
        // Newton iteration doubles correct bits at each step.
        inverse *= 2 - x * inverse;
        inverse *= 2 - x * inverse;
        inverse *= 2 - x * inverse;
        return inverse;
    }

    @Override
    public String toString() {
        return "TimeWindow{" + "start=" + start + ", end=" + end + '}';
    }

    /** Returns {@code true} if this window intersects the given window. */
    public boolean intersects(TimeWindow other) {
        return this.start <= other.end && this.end >= other.start;
    }

    /** Returns the minimal window covers both this window and the given window. */
    public TimeWindow cover(TimeWindow other) {
        return new TimeWindow(Math.min(start, other.start), Math.max(end, other.end));
    }

    @Override
    public int compareTo(Window o) {
        TimeWindow that = (TimeWindow) o;
        if (this.start == that.start) {
            return Long.compare(this.end, that.end);
        } else {
            return Long.compare(this.start, that.start);
        }
    }

    // ------------------------------------------------------------------------
    // Serializer
    // ------------------------------------------------------------------------

    /** The serializer used to write the TimeWindow type. */
    public static class Serializer extends TypeSerializerSingleton<TimeWindow> {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public TimeWindow createInstance() {
            return null;
        }

        @Override
        public TimeWindow copy(TimeWindow from) {
            return from;
        }

        @Override
        public TimeWindow copy(TimeWindow from, TimeWindow reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return 0;
        }

        @Override
        public void serialize(TimeWindow record, DataOutputView target) throws IOException {
            target.writeLong(record.start);
            target.writeLong(record.end);
        }

        @Override
        public TimeWindow deserialize(DataInputView source) throws IOException {
            long start = source.readLong();
            long end = source.readLong();
            return new TimeWindow(start, end);
        }

        @Override
        public TimeWindow deserialize(TimeWindow reuse, DataInputView source) throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            target.writeLong(source.readLong());
            target.writeLong(source.readLong());
        }

        // ------------------------------------------------------------------------

        @Override
        public TypeSerializerSnapshot<TimeWindow> snapshotConfiguration() {
            return new TimeWindow.Serializer.TimeWindowSerializerSnapshot();
        }

        /** Serializer configuration snapshot for compatibility and format evolution. */
        @SuppressWarnings("WeakerAccess")
        public static final class TimeWindowSerializerSnapshot
                extends SimpleTypeSerializerSnapshot<TimeWindow> {

            public TimeWindowSerializerSnapshot() {
                super(TimeWindow.Serializer::new);
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /**
     * Method to get the window start for a timestamp.
     *
     * @param timestamp epoch millisecond to get the window start.
     * @param offset The offset which window start would be shifted by.
     * @param windowSize The size of the generated windows.
     * @return window start
     */
    public static long getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
        final long remainder = (timestamp - offset) % windowSize;
        // handle both positive and negative cases
        if (remainder < 0) {
            return timestamp - (remainder + windowSize);
        } else {
            return timestamp - remainder;
        }
    }

    public static TimeWindow of(long start, long end) {
        return new TimeWindow(start, end);
    }
}
