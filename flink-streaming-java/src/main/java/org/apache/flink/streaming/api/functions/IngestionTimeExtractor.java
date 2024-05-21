/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions;

import org.apache.flink.api.common.eventtime.TimestampWatermark;
import org.apache.flink.streaming.api.watermark.WatermarkEvent;

/**
 * A timestamp assigner that assigns timestamps based on the machine's wall clock.
 *
 * <p>If this assigner is used after a stream source, it realizes "ingestion time" semantics.
 *
 * @param <T> The elements that get timestamps assigned.
 */
@Deprecated
public class IngestionTimeExtractor<T> implements AssignerWithPeriodicWatermarks<T> {
    private static final long serialVersionUID = -4072216356049069301L;

    private long maxTimestamp;

    @Override
    public long extractTimestamp(T element, long previousElementTimestamp) {
        // make sure timestamps are monotonously increasing, even when the system clock re-syncs
        final long now = Math.max(System.currentTimeMillis(), maxTimestamp);
        maxTimestamp = now;
        return now;
    }

    @Override
    public WatermarkEvent getCurrentWatermark() {
        // make sure timestamps are monotonously increasing, even when the system clock re-syncs
        final long now = Math.max(System.currentTimeMillis(), maxTimestamp);
        maxTimestamp = now;
        return new WatermarkEvent(new TimestampWatermark(now - 1));
    }
}
