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

package org.apache.flink.table.runtime.generated;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.watermark.WatermarkEmitStrategy;
import org.apache.flink.table.watermark.WatermarkParams;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Wrapper of the {@link GeneratedWatermarkGenerator} that is used to create {@link
 * org.apache.flink.api.common.eventtime.WatermarkGenerator}. The {@link
 * GeneratedWatermarkGeneratorSupplier} uses the {@link Context} to init the generated watermark
 * generator.
 */
@Internal
public class GeneratedWatermarkGeneratorSupplier implements WatermarkGeneratorSupplier<RowData> {
    private static final long serialVersionUID = 1L;

    private final GeneratedWatermarkGenerator generatedWatermarkGenerator;
    private final WatermarkParams watermarkParams;

    public GeneratedWatermarkGeneratorSupplier(
            GeneratedWatermarkGenerator generatedWatermarkGenerator,
            @Nullable WatermarkParams watermarkParams) {
        this.generatedWatermarkGenerator = generatedWatermarkGenerator;
        this.watermarkParams = watermarkParams;
    }

    @Override
    public org.apache.flink.api.common.eventtime.WatermarkGenerator<RowData>
            createWatermarkGenerator(Context context) {

        List<Object> references =
                new ArrayList<>(Arrays.asList(generatedWatermarkGenerator.getReferences()));
        references.add(context);

        WatermarkGenerator innerWatermarkGenerator =
                new GeneratedWatermarkGenerator(
                                generatedWatermarkGenerator.getClassName(),
                                generatedWatermarkGenerator.getCode(),
                                references.toArray())
                        .newInstance(Thread.currentThread().getContextClassLoader());

        try {
            innerWatermarkGenerator.open(DefaultOpenContext.INSTANCE);
        } catch (Exception e) {
            throw new RuntimeException("Fail to instantiate generated watermark generator.", e);
        }

        WatermarkEmitStrategy watermarkEmitStrategy =
                watermarkParams == null
                        ? WatermarkEmitStrategy.ON_PERIODIC
                        : watermarkParams.getEmitStrategy();
        return new GeneratedWatermarkGeneratorSupplier.DefaultWatermarkGenerator(
                innerWatermarkGenerator, watermarkEmitStrategy);
    }

    /** Wrapper of the code-generated {@link WatermarkGenerator}. */
    public static class DefaultWatermarkGenerator
            implements org.apache.flink.api.common.eventtime.WatermarkGenerator<RowData> {
        private static final long serialVersionUID = 1L;

        private final WatermarkGenerator innerWatermarkGenerator;
        private final org.apache.flink.table.watermark.WatermarkEmitStrategy watermarkEmitStrategy;
        private Long currentWatermark = Long.MIN_VALUE;

        public DefaultWatermarkGenerator(
                WatermarkGenerator watermarkGenerator,
                org.apache.flink.table.watermark.WatermarkEmitStrategy watermarkEmitStrategy) {
            this.innerWatermarkGenerator = watermarkGenerator;
            this.watermarkEmitStrategy = watermarkEmitStrategy;
        }

        @Override
        public void onEvent(RowData event, long eventTimestamp, WatermarkOutput output) {
            try {
                Long watermark = innerWatermarkGenerator.currentWatermark(event);
                if (watermark != null) {
                    currentWatermark = watermark;
                    if (watermarkEmitStrategy.isOnEvent()) {
                        output.emitWatermark(new Watermark(currentWatermark));
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(
                        String.format(
                                "Generated WatermarkGenerator fails to generate for row: %s.",
                                event),
                        e);
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            if (watermarkEmitStrategy.isOnPeriodic()) {
                output.emitWatermark(new Watermark(currentWatermark));
            }
        }
    }
}
