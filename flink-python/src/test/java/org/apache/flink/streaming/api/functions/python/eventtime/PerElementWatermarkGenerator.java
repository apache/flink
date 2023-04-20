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

package org.apache.flink.streaming.api.functions.python.eventtime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/** PerElementWatermarkGenerator that generates max watermark for every arriving item. */
@Internal
public class PerElementWatermarkGenerator implements WatermarkGenerator<Object> {

    private long maxTimestamp;

    public PerElementWatermarkGenerator() {
        maxTimestamp = Long.MIN_VALUE;
    }

    @Override
    public void onEvent(Object event, long eventTimestamp, WatermarkOutput output) {
        if (eventTimestamp > maxTimestamp) {
            maxTimestamp = eventTimestamp;
            output.emitWatermark(new Watermark(eventTimestamp));
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {}

    public static WatermarkGeneratorSupplier<Object> getSupplier() {
        return (ctx) -> new PerElementWatermarkGenerator();
    }
}
