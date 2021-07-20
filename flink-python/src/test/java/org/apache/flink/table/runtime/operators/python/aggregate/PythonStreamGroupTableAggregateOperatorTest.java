/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.python.aggregate;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.python.PythonAggregateFunctionInfo;
import org.apache.flink.table.planner.typeutils.DataViewUtils;
import org.apache.flink.table.runtime.operators.python.scalar.PythonScalarFunctionOperatorTestBase;
import org.apache.flink.table.runtime.utils.PassThroughStreamTableAggregatePythonFunctionRunner;
import org.apache.flink.table.runtime.utils.PythonTestUtils;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;

/** The tests for {@link PythonStreamGroupTableAggregateOperator}. */
public class PythonStreamGroupTableAggregateOperatorTest
        extends AbstractPythonStreamAggregateOperatorTest {

    @Test
    public void testFlushDataOnClose() throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                getTestHarness(new Configuration());
        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        testHarness.processElement(new StreamRecord<>(newRow(true, "c1", 0L), initialTime + 1));
        testHarness.processElement(new StreamRecord<>(newRow(false, "c2", 1L), initialTime + 2));
        testHarness.close();

        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", 0L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", 0L)));
        expectedOutput.add(new StreamRecord<>(newRow(false, "c2", 1L)));
        expectedOutput.add(new StreamRecord<>(newRow(false, "c2", 1L)));

        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
    }

    @Test
    public void testFinishBundleTriggeredOnCheckpoint() throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger(PythonOptions.MAX_BUNDLE_SIZE, 10);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = getTestHarness(conf);

        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        testHarness.processElement(new StreamRecord<>(newRow(true, "c1", 0L), initialTime + 1));
        testHarness.processElement(new StreamRecord<>(newRow(true, "c2", 1L), initialTime + 2));
        testHarness.processElement(new StreamRecord<>(newRow(true, "c3", 2L), initialTime + 3));
        // checkpoint trigger finishBundle
        testHarness.prepareSnapshotPreBarrier(0L);

        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", 0L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", 0L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c2", 1L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c2", 1L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c3", 2L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c3", 2L)));

        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    @Test
    public void testFinishBundleTriggeredByCount() throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger(PythonOptions.MAX_BUNDLE_SIZE, 3);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = getTestHarness(conf);

        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        testHarness.processElement(new StreamRecord<>(newRow(true, "c1", 0L), initialTime + 1));
        testHarness.processElement(new StreamRecord<>(newRow(true, "c2", 1L), initialTime + 2));
        assertOutputEquals(
                "FinishBundle should not be triggered.", expectedOutput, testHarness.getOutput());

        testHarness.processElement(new StreamRecord<>(newRow(true, "c3", 2L), initialTime + 2));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", 0L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", 0L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c2", 1L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c2", 1L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c3", 2L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c3", 2L)));

        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    @Test
    public void testFinishBundleTriggeredByTime() throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger(PythonOptions.MAX_BUNDLE_SIZE, 10);
        conf.setLong(PythonOptions.MAX_BUNDLE_TIME_MILLS, 1000L);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = getTestHarness(conf);

        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        testHarness.processElement(new StreamRecord<>(newRow(true, "c1", 0L), initialTime + 1));
        testHarness.processElement(new StreamRecord<>(newRow(true, "c2", 1L), initialTime + 2));
        testHarness.processElement(new StreamRecord<>(newRow(true, "c3", 2L), initialTime + 3));
        assertOutputEquals(
                "FinishBundle should not be triggered.", expectedOutput, testHarness.getOutput());

        testHarness.setProcessingTime(1000L);
        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", 0L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", 0L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c2", 1L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c2", 1L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c3", 2L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c3", 2L)));
        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    @Test
    public void testWatermarkProcessedOnFinishBundle() throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger(PythonOptions.MAX_BUNDLE_SIZE, 10);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = getTestHarness(conf);
        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        testHarness.processElement(new StreamRecord<>(newRow(true, "c1", 0L), initialTime + 1));
        testHarness.processElement(new StreamRecord<>(newRow(true, "c2", 1L), initialTime + 2));
        testHarness.processWatermark(initialTime + 2);
        assertOutputEquals("Watermark has been processed", expectedOutput, testHarness.getOutput());

        // checkpoint trigger finishBundle
        testHarness.prepareSnapshotPreBarrier(0L);

        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", 0L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", 0L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c2", 1L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c2", 1L)));
        expectedOutput.add(new Watermark(initialTime + 2));

        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    @Test
    public void testStateCleanupTimer() throws Exception {
        Configuration conf = new Configuration();
        conf.setString("table.exec.state.ttl", "100");
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = getTestHarness(conf);

        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        testHarness.setProcessingTime(0L);
        testHarness.processElement(new StreamRecord<>(newRow(true, "c1", 0L), initialTime + 1));
        testHarness.setProcessingTime(500L);
        testHarness.processElement(new StreamRecord<>(newRow(true, "c2", 1L), initialTime + 2));
        testHarness.setProcessingTime(599L);
        testHarness.processElement(new StreamRecord<>(newRow(true, "c2", 2L), initialTime + 3));
        testHarness.setProcessingTime(1000L);

        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", 0L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", 0L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "state_cleanup_triggered: c1", 100L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c2", 1L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c2", 1L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c2", 2L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c2", 2L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "state_cleanup_triggered: c2", 699L)));

        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    @Override
    public OneInputStreamOperator getTestOperator(Configuration config) {
        long stateTtl = Long.valueOf(config.getString("table.exec.state.ttl", "0"));
        return new PassThroughPythonStreamGroupTableAggregateOperator(
                config,
                getInputType(),
                getOutputType(),
                new PythonAggregateFunctionInfo[] {
                    new PythonAggregateFunctionInfo(
                            PythonScalarFunctionOperatorTestBase.DummyPythonFunction.INSTANCE,
                            new Integer[] {0},
                            -1,
                            false)
                },
                getGrouping(),
                -1,
                false,
                stateTtl,
                stateTtl);
    }

    private static class PassThroughPythonStreamGroupTableAggregateOperator
            extends PythonStreamGroupTableAggregateOperator {

        PassThroughPythonStreamGroupTableAggregateOperator(
                Configuration config,
                RowType inputType,
                RowType outputType,
                PythonAggregateFunctionInfo[] aggregateFunctions,
                int[] grouping,
                int indexOfCountStar,
                boolean generateUpdateBefore,
                long minRetentionTime,
                long maxRetentionTime) {
            super(
                    config,
                    inputType,
                    outputType,
                    aggregateFunctions,
                    new DataViewUtils.DataViewSpec[0][0],
                    grouping,
                    indexOfCountStar,
                    generateUpdateBefore,
                    minRetentionTime,
                    maxRetentionTime);
        }

        @Override
        public PythonFunctionRunner createPythonFunctionRunner() {
            return new PassThroughStreamTableAggregatePythonFunctionRunner(
                    getRuntimeContext().getTaskName(),
                    PythonTestUtils.createTestEnvironmentManager(),
                    userDefinedFunctionInputType,
                    outputType,
                    STREAM_GROUP_TABLE_AGGREGATE_URN,
                    getUserDefinedFunctionsProto(),
                    new HashMap<>(),
                    PythonTestUtils.createMockFlinkMetricContainer(),
                    getKeyedStateBackend(),
                    getKeySerializer(),
                    getProcessFunction());
        }

        private Function<byte[], byte[][]> getProcessFunction() {
            return (input_bytes) -> {
                try {
                    RowData input =
                            udfInputTypeSerializer.deserialize(
                                    new DataInputDeserializer(input_bytes));
                    DataOutputSerializer output = new DataOutputSerializer(1);
                    if (input.getByte(0) == NORMAL_RECORD) {
                        RowData data = input.getRow(1, inputType.getFieldCount());
                        udfOutputTypeSerializer.serialize(data, output);
                        byte[] result = output.getCopyOfBuffer();
                        return new byte[][] {result, result};
                    } else {
                        udfOutputTypeSerializer.serialize(
                                GenericRowData.of(
                                        StringData.fromString(
                                                "state_cleanup_triggered: "
                                                        + input.getRow(
                                                                        3,
                                                                        getKeyType()
                                                                                .getFieldCount())
                                                                .getString(0)),
                                        input.getLong(2)),
                                output);
                        return new byte[][] {output.getCopyOfBuffer()};
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            };
        }
    }
}
