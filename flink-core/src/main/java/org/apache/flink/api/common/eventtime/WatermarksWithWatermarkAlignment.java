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

package org.apache.flink.api.common.eventtime;

import org.apache.flink.annotation.Internal;

import java.time.Duration;

/** A helper class to pass a watermark group and max allowed watermark drift to the runtime. */
@Internal
final class WatermarksWithWatermarkAlignment<T> implements WatermarkStrategy<T> {

    static final Duration DEFAULT_UPDATE_INTERVAL = Duration.ofMillis(1000);

    private final WatermarkStrategy<T> strategy;

    private final String watermarkGroup;

    private final Duration maxAllowedWatermarkDrift;

    private final Duration updateInterval;

    public WatermarksWithWatermarkAlignment(
            WatermarkStrategy<T> strategy,
            String watermarkGroup,
            Duration maxAllowedWatermarkDrift,
            Duration updateInterval) {
        this.strategy = strategy;
        this.watermarkGroup = watermarkGroup;
        this.maxAllowedWatermarkDrift = maxAllowedWatermarkDrift;
        this.updateInterval = updateInterval;
    }

    @Override
    public TimestampAssigner<T> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return strategy.createTimestampAssigner(context);
    }

    @Override
    public WatermarkGenerator<T> createWatermarkGenerator(
            WatermarkGeneratorSupplier.Context context) {
        return strategy.createWatermarkGenerator(context);
    }

    @Override
    public WatermarkAlignmentParams getAlignmentParameters() {
        return new WatermarkAlignmentParams(
                maxAllowedWatermarkDrift.toMillis(), watermarkGroup, updateInterval.toMillis());
    }
}
