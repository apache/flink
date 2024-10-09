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

package org.apache.flink.table.runtime.operators.over;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.operators.sort.IntRecordComparator;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.OutputTag;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Test for {@link NonBufferOverWindowOperator}. */
class NonBufferOverWindowOperatorTest {

    static GeneratedAggsHandleFunction function =
            new GeneratedAggsHandleFunction("Function1", "", new Object[0]) {
                @Override
                public AggsHandleFunction newInstance(ClassLoader classLoader) {
                    return new SumAggsHandleFunction(1);
                }
            };
    static GeneratedRecordComparator comparator =
            new GeneratedRecordComparator("Comparator", "", new Object[0]) {
                @Override
                public RecordComparator newInstance(ClassLoader classLoader) {
                    return new IntRecordComparator();
                }
            };
    static RowType inputType = RowType.of(new IntType(), new BigIntType(), new BigIntType());
    static RowDataSerializer inputSer = new RowDataSerializer(inputType);

    private static GeneratedAggsHandleFunction[] functions;

    static {
        GeneratedAggsHandleFunction function2 =
                new GeneratedAggsHandleFunction("Function2", "", new Object[0]) {
                    @Override
                    public AggsHandleFunction newInstance(ClassLoader classLoader) {
                        return new SumAggsHandleFunction(2);
                    }
                };
        functions = new GeneratedAggsHandleFunction[] {function, function2};
    }

    private NonBufferOverWindowOperator operator;
    private List<GenericRowData> collect;

    @BeforeEach
    void before() throws Exception {
        collect = new ArrayList<>();
    }

    @Test
    void testNormal() throws Exception {
        test(
                new boolean[] {false, false},
                new GenericRowData[] {
                    GenericRowData.of(0, 1L, 4L, 1L, 4L),
                    GenericRowData.of(0, 1L, 1L, 2L, 5L),
                    GenericRowData.of(1, 5L, 2L, 5L, 2L),
                    GenericRowData.of(2, 5L, 4L, 5L, 4L),
                    GenericRowData.of(2, 6L, 2L, 11L, 6L)
                });
    }

    @Test
    void testResetAccumulators() throws Exception {
        test(
                new boolean[] {true, false},
                new GenericRowData[] {
                    GenericRowData.of(0, 1L, 4L, 1L, 4L),
                    GenericRowData.of(0, 1L, 1L, 1L, 5L),
                    GenericRowData.of(1, 5L, 2L, 5L, 2L),
                    GenericRowData.of(2, 5L, 4L, 5L, 4L),
                    GenericRowData.of(2, 6L, 2L, 6L, 6L)
                });
    }

    private void test(boolean[] resetAccumulators, GenericRowData[] expect) throws Exception {
        MockEnvironment env = new MockEnvironmentBuilder().build();
        StreamTask<Object, StreamOperator<Object>> task =
                new StreamTask<Object, StreamOperator<Object>>(env) {
                    @Override
                    protected void init() {}
                };
        StreamConfig streamConfig = mock(StreamConfig.class);
        when(streamConfig.<RowData>getTypeSerializerIn1(
                        Thread.currentThread().getContextClassLoader()))
                .thenReturn(inputSer);
        when(streamConfig.getManagedMemoryFractionOperatorUseCaseOfSlot(
                        eq(ManagedMemoryUseCase.OPERATOR),
                        any(Configuration.class),
                        any(Configuration.class),
                        any(ClassLoader.class)))
                .thenReturn(0.99);
        when(streamConfig.getOperatorID()).thenReturn(new OperatorID());
        operator =
                new NonBufferOverWindowOperator(
                        new StreamOperatorParameters<>(
                                task,
                                streamConfig,
                                new ConsumerOutput(
                                        r ->
                                                collect.add(
                                                        GenericRowData.of(
                                                                r.getInt(0),
                                                                r.getLong(1),
                                                                r.getLong(2),
                                                                r.getLong(3),
                                                                r.getLong(4)))),
                                TestProcessingTimeService::new,
                                null,
                                null),
                        functions,
                        comparator,
                        resetAccumulators) {
                    public StreamingRuntimeContext getRuntimeContext() {
                        return mock(StreamingRuntimeContext.class);
                    }
                };
        operator.open();
        addRow(0, 1L, 4L);
        addRow(0, 1L, 1L);
        addRow(1, 5L, 2L);
        addRow(2, 5L, 4L);
        addRow(2, 6L, 2L);
        GenericRowData[] outputs = this.collect.toArray(new GenericRowData[0]);
        assertThat(outputs).isEqualTo(expect);
    }

    private void addRow(Object... fields) throws Exception {
        operator.processElement(new StreamRecord<>(GenericRowData.of(fields)));
    }

    /** Output of Consumer. */
    static class ConsumerOutput implements Output<StreamRecord<RowData>> {

        private final Consumer<RowData> consumer;

        public ConsumerOutput(Consumer<RowData> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void emitWatermark(Watermark mark) {
            throw new RuntimeException();
        }

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {
            throw new RuntimeException();
        }

        @Override
        public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
            throw new RuntimeException();
        }

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) {
            throw new RuntimeException();
        }

        @Override
        public void emitRecordAttributes(RecordAttributes recordAttributes) {
            throw new RuntimeException();
        }

        @Override
        public void collect(StreamRecord<RowData> record) {
            consumer.accept(record.getValue());
        }

        @Override
        public void close() {}
    }
}
