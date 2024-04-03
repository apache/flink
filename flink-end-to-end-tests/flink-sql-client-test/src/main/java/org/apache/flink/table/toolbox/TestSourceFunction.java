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

package org.apache.flink.table.toolbox;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A source function used to test.
 *
 * <p>For simplicity, the deprecated source function method is used to create the source.
 */
@SuppressWarnings("deprecation")
public class TestSourceFunction implements SourceFunction<RowData> {

    public static final List<List<Object>> DATA = new ArrayList<>();

    static {
        DATA.add(Arrays.asList(StringData.fromString("Bob"), 1L, TimestampData.fromEpochMillis(1)));
        DATA.add(
                Arrays.asList(
                        StringData.fromString("Alice"), 2L, TimestampData.fromEpochMillis(2)));
    }

    private final WatermarkStrategy<RowData> watermarkStrategy;

    public TestSourceFunction(WatermarkStrategy<RowData> watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
    }

    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<RowData> ctx) {
        WatermarkGenerator<RowData> generator =
                watermarkStrategy.createWatermarkGenerator(() -> null);
        WatermarkOutput output = new TestWatermarkOutput(ctx);

        int rowDataSize = DATA.get(0).size();
        int index = 0;
        while (index < DATA.size() && isRunning) {
            List<Object> list = DATA.get(index);
            GenericRowData row = new GenericRowData(rowDataSize);
            for (int i = 0; i < rowDataSize; i++) {
                row.setField(i, list.get(i));
            }
            generator.onEvent(row, Long.MIN_VALUE, output);
            generator.onPeriodicEmit(output);

            ctx.collect(row);

            index++;
        }
        ctx.close();
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    private static class TestWatermarkOutput implements WatermarkOutput {

        private final SourceFunction.SourceContext<RowData> ctx;

        public TestWatermarkOutput(SourceFunction.SourceContext<RowData> ctx) {
            this.ctx = ctx;
        }

        @Override
        public void emitWatermark(Watermark watermark) {
            ctx.emitWatermark(
                    new org.apache.flink.streaming.api.watermark.Watermark(
                            watermark.getTimestamp()));
        }

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}
    }
}
