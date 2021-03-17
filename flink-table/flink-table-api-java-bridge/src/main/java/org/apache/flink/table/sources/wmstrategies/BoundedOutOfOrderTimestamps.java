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

package org.apache.flink.table.sources.wmstrategies;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.descriptors.Rowtime;

import java.util.HashMap;
import java.util.Map;

/**
 * A watermark strategy for rowtime attributes which are out-of-order by a bounded time interval.
 *
 * <p>Emits watermarks which are the maximum observed timestamp minus the specified delay.
 */
@PublicEvolving
public final class BoundedOutOfOrderTimestamps extends PeriodicWatermarkAssigner {

    private static final long serialVersionUID = 1L;

    private final long delay;
    private long maxTimestamp;

    /** @param delay The delay by which watermarks are behind the maximum observed timestamp. */
    public BoundedOutOfOrderTimestamps(long delay) {
        this.delay = delay;
        maxTimestamp = Long.MIN_VALUE + delay;
    }

    @Override
    public void nextTimestamp(long timestamp) {
        if (timestamp > maxTimestamp) {
            maxTimestamp = timestamp;
        }
    }

    @Override
    public Watermark getWatermark() {
        return new Watermark(maxTimestamp - delay);
    }

    @Override
    public Map<String, String> toProperties() {
        Map<String, String> map = new HashMap<>();
        map.put(
                Rowtime.ROWTIME_WATERMARKS_TYPE,
                Rowtime.ROWTIME_WATERMARKS_TYPE_VALUE_PERIODIC_BOUNDED);
        map.put(Rowtime.ROWTIME_WATERMARKS_DELAY, String.valueOf(delay));
        return map;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BoundedOutOfOrderTimestamps that = (BoundedOutOfOrderTimestamps) o;

        return delay == that.delay;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(delay);
    }
}
