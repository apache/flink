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

package org.apache.flink.table.planner.factories.source;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.factories.TestValuesRuntimeHelper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedList;

/** The source implementation collects {@link RowData} elements and records watermark. */
public class FromElementSourceFunctionWithWatermark implements SourceFunction<RowData> {

    /** The (de)serializer to be used for the data elements. */
    private final TypeSerializer<RowData> serializer;

    /** The actual data elements, in serialized form. */
    private final byte[] elementsSerialized;

    /** The number of serialized elements. */
    private final int numElements;

    /** The number of elements emitted already. */
    private volatile int numElementsEmitted;

    /** WatermarkStrategy to generate watermark generator. */
    private final WatermarkStrategy<RowData> watermarkStrategy;

    private volatile boolean isRunning = true;

    private String tableName;

    public FromElementSourceFunctionWithWatermark(
            String tableName,
            TypeSerializer<RowData> serializer,
            Iterable<RowData> elements,
            WatermarkStrategy<RowData> watermarkStrategy)
            throws IOException {
        this.tableName = tableName;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos);

        int count = 0;
        try {
            for (RowData element : elements) {
                serializer.serialize(element, wrapper);
                count++;
            }
        } catch (Exception e) {
            throw new IOException("Serializing the source elements failed: " + e.getMessage(), e);
        }

        this.numElements = count;
        this.elementsSerialized = baos.toByteArray();
        this.watermarkStrategy = watermarkStrategy;
        this.serializer = serializer;
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        ByteArrayInputStream bais = new ByteArrayInputStream(elementsSerialized);
        final DataInputView input = new DataInputViewStreamWrapper(bais);
        WatermarkGenerator<RowData> generator =
                watermarkStrategy.createWatermarkGenerator(() -> null);
        WatermarkOutput output = new TestValuesWatermarkOutput(ctx);
        final Object lock = ctx.getCheckpointLock();

        while (isRunning && numElementsEmitted < numElements) {
            RowData next;
            try {
                next = serializer.deserialize(input);
                generator.onEvent(next, Long.MIN_VALUE, output);
                generator.onPeriodicEmit(output);
            } catch (Exception e) {
                throw new IOException(
                        "Failed to deserialize an element from the source. "
                                + "If you are using user-defined serialization (Value and Writable types), check the "
                                + "serialization functions.\nSerializer is "
                                + serializer,
                        e);
            }

            synchronized (lock) {
                ctx.collect(next);
                numElementsEmitted++;
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    private class TestValuesWatermarkOutput implements WatermarkOutput {
        SourceContext<RowData> ctx;

        public TestValuesWatermarkOutput(SourceContext<RowData> ctx) {
            this.ctx = ctx;
        }

        @Override
        public void emitWatermark(Watermark watermark) {
            ctx.emitWatermark(
                    new org.apache.flink.streaming.api.watermark.Watermark(
                            watermark.getTimestamp()));
            synchronized (TestValuesRuntimeHelper.LOCK) {
                TestValuesRuntimeHelper.getWatermarkHistory()
                        .computeIfAbsent(tableName, k -> new LinkedList<>())
                        .add(watermark);
            }
        }

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}
    }
}
