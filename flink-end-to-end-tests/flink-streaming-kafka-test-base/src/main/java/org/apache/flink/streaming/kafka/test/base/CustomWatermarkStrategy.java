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

package org.apache.flink.streaming.kafka.test.base;

import org.apache.flink.api.common.eventtime.AscendingTimestampsWatermarks;
import org.apache.flink.api.common.eventtime.BoundedOutOfOrdernessWatermarks;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

/**
 * A custom {@link WatermarkStrategy}, that simply assumes that the input stream records are
 * strictly ascending.
 *
 * <p>Flink also ships some built-in convenience watermark strategies, such as the {@link
 * BoundedOutOfOrdernessWatermarks} and {@link AscendingTimestampsWatermarks}
 */
public class CustomWatermarkStrategy implements WatermarkStrategy<KafkaEvent> {

    private static final long serialVersionUID = -6473078087120012424L;

    private long currentTimestamp = Long.MIN_VALUE;

    @Override
    public TimestampAssigner<KafkaEvent> createTimestampAssigner(
            TimestampAssignerSupplier.Context context) {
        return (event, recordTimestamp) -> {
            // the inputs are assumed to be of format (message,timestamp)
            currentTimestamp = event.getTimestamp();
            return event.getTimestamp();
        };
    }

    @Override
    public WatermarkGenerator<KafkaEvent> createWatermarkGenerator(
            WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<KafkaEvent>() {
            @Override
            public void onEvent(KafkaEvent event, long eventTimestamp, WatermarkOutput output) {
                output.emitWatermark(
                        new Watermark(
                                currentTimestamp == Long.MIN_VALUE
                                        ? Long.MIN_VALUE
                                        : currentTimestamp - 1));
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput output) {}
        };
    }
}
