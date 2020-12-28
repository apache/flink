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

package org.apache.flink.table.runtime.operators.python.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.operators.python.scalar.PythonScalarFunctionOperatorTestBase;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.calcite.rel.core.JoinRelType;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Base class for Python table function operator test. These test that:
 *
 * <ul>
 *   <li>Retraction flag is correctly forwarded to the downstream
 *   <li>FinishBundle is called when checkpoint is encountered
 *   <li>Watermarks are buffered and only sent to downstream when finishedBundle is triggered
 * </ul>
 *
 * @param <IN> Type of the input elements.
 * @param <OUT> Type of the output elements.
 * @param <UDTFIN> Type of the UDTF input type.
 */
public abstract class PythonTableFunctionOperatorTestBase<IN, OUT, UDTFIN> {

    @Test
    public void testRetractionFieldKept() throws Exception {
        OneInputStreamOperatorTestHarness<IN, OUT> testHarness =
                getTestHarness(new Configuration(), JoinRelType.INNER);
        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        testHarness.processElement(
                new StreamRecord<>(newRow(true, "c1", "c2", 0L), initialTime + 1));
        testHarness.processElement(
                new StreamRecord<>(newRow(false, "c3", "c4", 1L), initialTime + 2));
        testHarness.processElement(
                new StreamRecord<>(newRow(false, "c5", "c6", 2L), initialTime + 3));
        testHarness.close();

        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c2", 0L, 0L)));
        expectedOutput.add(new StreamRecord<>(newRow(false, "c3", "c4", 1L, 1L)));
        expectedOutput.add(new StreamRecord<>(newRow(false, "c5", "c6", 2L, 2L)));

        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
    }

    @Test
    public void testFinishBundleTriggeredOnCheckpoint() throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger(PythonOptions.MAX_BUNDLE_SIZE, 10);
        OneInputStreamOperatorTestHarness<IN, OUT> testHarness =
                getTestHarness(conf, JoinRelType.INNER);

        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        testHarness.processElement(
                new StreamRecord<>(newRow(true, "c1", "c2", 0L), initialTime + 1));

        // checkpoint trigger finishBundle
        testHarness.prepareSnapshotPreBarrier(0L);

        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c2", 0L, 0L)));

        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    @Test
    public void testFinishBundleTriggeredByCount() throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger(PythonOptions.MAX_BUNDLE_SIZE, 2);
        OneInputStreamOperatorTestHarness<IN, OUT> testHarness =
                getTestHarness(conf, JoinRelType.INNER);

        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        testHarness.processElement(
                new StreamRecord<>(newRow(true, "c1", "c2", 0L), initialTime + 1));
        assertOutputEquals(
                "FinishBundle should not be triggered.", expectedOutput, testHarness.getOutput());

        testHarness.processElement(
                new StreamRecord<>(newRow(true, "c1", "c2", 1L), initialTime + 2));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c2", 0L, 0L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c2", 1L, 1L)));

        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    @Test
    public void testFinishBundleTriggeredByTime() throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger(PythonOptions.MAX_BUNDLE_SIZE, 10);
        conf.setLong(PythonOptions.MAX_BUNDLE_TIME_MILLS, 1000L);
        OneInputStreamOperatorTestHarness<IN, OUT> testHarness =
                getTestHarness(conf, JoinRelType.INNER);

        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        testHarness.processElement(
                new StreamRecord<>(newRow(true, "c1", "c2", 0L), initialTime + 1));
        assertOutputEquals(
                "FinishBundle should not be triggered.", expectedOutput, testHarness.getOutput());

        testHarness.setProcessingTime(1000L);
        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c2", 0L, 0L)));
        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    @Test
    public void testLeftJoin() throws Exception {
        OneInputStreamOperatorTestHarness<IN, OUT> testHarness =
                getTestHarness(new Configuration(), JoinRelType.LEFT);
        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        testHarness.processElement(
                new StreamRecord<>(newRow(true, "c1", "c2", 0L), initialTime + 1));
        testHarness.processElement(
                new StreamRecord<>(newRow(false, "c2", "c4", 1L), initialTime + 2));
        testHarness.processElement(
                new StreamRecord<>(newRow(false, "c3", "c6", 2L), initialTime + 3));
        testHarness.processElement(
                new StreamRecord<>(newRow(false, "c4", "c6", 2L), initialTime + 4));
        testHarness.processElement(
                new StreamRecord<>(newRow(false, "c5", "c6", 2L), initialTime + 5));
        testHarness.processElement(
                new StreamRecord<>(newRow(false, "c6", "c6", 2L), initialTime + 6));
        testHarness.processElement(
                new StreamRecord<>(newRow(false, "c7", "c6", 2L), initialTime + 7));
        testHarness.processElement(
                new StreamRecord<>(newRow(false, "c8", "c6", 2L), initialTime + 8));
        testHarness.close();

        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c2", 0L, 0L)));
        expectedOutput.add(new StreamRecord<>(newRow(false, "c2", "c4", 1L, 1L)));
        expectedOutput.add(new StreamRecord<>(newRow(false, "c3", "c6", 2L, 2L)));
        expectedOutput.add(new StreamRecord<>(newRow(false, "c4", "c6", 2L, 2L)));
        expectedOutput.add(new StreamRecord<>(newRow(false, "c5", "c6", 2L, 2L)));
        expectedOutput.add(new StreamRecord<>(newRow(false, "c6", "c6", 2L, null)));
        expectedOutput.add(new StreamRecord<>(newRow(false, "c7", "c6", 2L, 2L)));
        expectedOutput.add(new StreamRecord<>(newRow(false, "c8", "c6", 2L, null)));

        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
    }

    private OneInputStreamOperatorTestHarness<IN, OUT> getTestHarness(
            Configuration config, JoinRelType joinRelType) throws Exception {
        RowType inputType =
                new RowType(
                        Arrays.asList(
                                new RowType.RowField("f1", new VarCharType()),
                                new RowType.RowField("f2", new VarCharType()),
                                new RowType.RowField("f3", new BigIntType())));
        RowType outputType =
                new RowType(
                        Arrays.asList(
                                new RowType.RowField("f1", new VarCharType()),
                                new RowType.RowField("f2", new VarCharType()),
                                new RowType.RowField("f3", new BigIntType()),
                                new RowType.RowField("f4", new BigIntType())));
        AbstractPythonTableFunctionOperator<IN, OUT, UDTFIN> operator =
                getTestOperator(
                        config,
                        new PythonFunctionInfo(
                                PythonScalarFunctionOperatorTestBase.DummyPythonFunction.INSTANCE,
                                new Integer[] {0}),
                        inputType,
                        outputType,
                        new int[] {2},
                        joinRelType);

        OneInputStreamOperatorTestHarness<IN, OUT> testHarness =
                new OneInputStreamOperatorTestHarness<>(operator);
        testHarness
                .getStreamConfig()
                .setManagedMemoryFractionOperatorOfUseCase(ManagedMemoryUseCase.PYTHON, 0.5);
        return testHarness;
    }

    public abstract IN newRow(boolean accumulateMsg, Object... fields);

    public abstract void assertOutputEquals(
            String message, Collection<Object> expected, Collection<Object> actual);

    public abstract AbstractPythonTableFunctionOperator<IN, OUT, UDTFIN> getTestOperator(
            Configuration config,
            PythonFunctionInfo tableFunction,
            RowType inputType,
            RowType outputType,
            int[] udfInputOffsets,
            JoinRelType joinRelType);
}
