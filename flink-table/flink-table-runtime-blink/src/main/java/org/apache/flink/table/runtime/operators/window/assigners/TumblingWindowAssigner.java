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

package org.apache.flink.table.runtime.operators.window.assigners;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.TimeWindow;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.table.runtime.operators.window.TimeWindow.getWindowStartWithOffset;

/**
 * A {@link WindowAssigner} that windows elements into fixed-size windows based on the timestamp of
 * the elements. Windows cannot overlap.
 */
public class TumblingWindowAssigner extends WindowAssigner<TimeWindow>
        implements InternalTimeWindowAssigner {

    private static final long serialVersionUID = -1671849072115929859L;
    /** Size of this window. */
    private final long size;

    /** Offset of this window. Windows start at time N * size + offset, where 0 is the epoch. */
    private final long offset;

    private final boolean isEventTime;

    protected TumblingWindowAssigner(long size, long offset, boolean isEventTime) {
        if (size <= 0) {
            throw new IllegalArgumentException(
                    "TumblingWindowAssigner parameters must satisfy size > 0");
        }
        this.size = size;
        this.offset = offset;
        this.isEventTime = isEventTime;
    }

    @Override
    public Collection<TimeWindow> assignWindows(RowData element, long timestamp) {
        long start = getWindowStartWithOffset(timestamp, offset, size);
        return Collections.singletonList(new TimeWindow(start, start + size));
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return isEventTime;
    }

    @Override
    public String toString() {
        return "TumblingWindow(" + size + ")";
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /**
     * Creates a new {@code TumblingWindowAssigner} {@link WindowAssigner} that assigns elements to
     * time windows based on the element timestamp.
     *
     * @param size The size of the generated windows.
     * @return The time policy.
     */
    public static TumblingWindowAssigner of(Duration size) {
        return new TumblingWindowAssigner(size.toMillis(), 0, true);
    }

    /**
     * Creates a new {@code TumblingWindowAssigner} {@link WindowAssigner} that assigns elements to
     * time windows based on the element timestamp and offset.
     *
     * <p>For example, if you want window a stream by hour,but window begins at the 15th minutes of
     * each hour, you can use {@code of(Time.hours(1),Time.minutes(15))},then you will get time
     * windows start at 0:15:00,1:15:00,2:15:00,etc.
     *
     * <p>Rather than that,if you are living in somewhere which is not using UTC±00:00 time, such as
     * China which is using GMT+08:00,and you want a time window with size of one day, and window
     * begins at every 00:00:00 of local time,you may use {@code of(Time.days(1),Time.hours(-8))}.
     * The parameter of offset is {@code Time.hours(-8))} since UTC+08:00 is 8 hours earlier than
     * UTC time.
     *
     * @param offset The offset which window start would be shifted by.
     * @return The time policy.
     */
    public TumblingWindowAssigner withOffset(Duration offset) {
        return new TumblingWindowAssigner(size, offset.toMillis(), isEventTime);
    }

    public TumblingWindowAssigner withEventTime() {
        return new TumblingWindowAssigner(size, offset, true);
    }

    public TumblingWindowAssigner withProcessingTime() {
        return new TumblingWindowAssigner(size, offset, false);
    }
}
