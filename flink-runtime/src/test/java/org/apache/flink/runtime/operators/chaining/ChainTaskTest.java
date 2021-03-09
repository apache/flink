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

package org.apache.flink.runtime.operators.chaining;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.BatchTask;
import org.apache.flink.runtime.operators.DataSourceTask;
import org.apache.flink.runtime.operators.DataSourceTaskTest;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.FlatMapDriver;
import org.apache.flink.runtime.operators.FlatMapTaskTest.MockMapStub;
import org.apache.flink.runtime.operators.ReduceTaskTest.MockCombiningReduceStub;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.testutils.TaskTestBase;
import org.apache.flink.runtime.operators.testutils.UniformRecordGenerator;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.testutils.recordutils.RecordComparatorFactory;
import org.apache.flink.runtime.testutils.recordutils.RecordSerializerFactory;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ChainTaskTest extends TaskTestBase {

    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    private static final int MEMORY_MANAGER_SIZE = 1024 * 1024 * 3;

    private static final int NETWORK_BUFFER_SIZE = 1024;

    private final List<Record> outList = new ArrayList<>();

    @SuppressWarnings("unchecked")
    private final RecordComparatorFactory compFact =
            new RecordComparatorFactory(
                    new int[] {0}, new Class[] {IntValue.class}, new boolean[] {true});

    private final RecordSerializerFactory serFact = RecordSerializerFactory.get();

    @Test
    public void testMapTask() {
        final int keyCnt = 100;
        final int valCnt = 20;

        final double memoryFraction = 1.0;

        try {

            // environment
            initEnvironment(MEMORY_MANAGER_SIZE, NETWORK_BUFFER_SIZE);
            addInput(new UniformRecordGenerator(keyCnt, valCnt, false), 0);
            addOutput(this.outList);

            // chained combine config
            {
                final TaskConfig combineConfig = new TaskConfig(new Configuration());

                // input
                combineConfig.addInputToGroup(0);
                combineConfig.setInputSerializer(serFact, 0);

                // output
                combineConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
                combineConfig.setOutputSerializer(serFact);

                // driver
                combineConfig.setDriverStrategy(DriverStrategy.SORTED_GROUP_COMBINE);
                combineConfig.setDriverComparator(compFact, 0);
                combineConfig.setDriverComparator(compFact, 1);
                combineConfig.setRelativeMemoryDriver(memoryFraction);

                // udf
                combineConfig.setStubWrapper(
                        new UserCodeClassWrapper<>(MockCombiningReduceStub.class));

                getTaskConfig()
                        .addChainedTask(
                                SynchronousChainedCombineDriver.class, combineConfig, "combine");
            }

            // chained map+combine
            {
                registerTask(FlatMapDriver.class, MockMapStub.class);
                BatchTask<FlatMapFunction<Record, Record>, Record> testTask =
                        new BatchTask<>(this.mockEnv);

                try {
                    testTask.invoke();
                } catch (Exception e) {
                    e.printStackTrace();
                    Assert.fail("Invoke method caused exception.");
                }
            }

            Assert.assertEquals(keyCnt, this.outList.size());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testFailingMapTask() {
        int keyCnt = 100;
        int valCnt = 20;

        final long memorySize = 1024 * 1024 * 3;
        final int bufferSize = 1014 * 1024;
        final double memoryFraction = 1.0;

        try {
            // environment
            initEnvironment(memorySize, bufferSize);
            addInput(new UniformRecordGenerator(keyCnt, valCnt, false), 0);
            addOutput(this.outList);

            // chained combine config
            {
                final TaskConfig combineConfig = new TaskConfig(new Configuration());

                // input
                combineConfig.addInputToGroup(0);
                combineConfig.setInputSerializer(serFact, 0);

                // output
                combineConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
                combineConfig.setOutputSerializer(serFact);

                // driver
                combineConfig.setDriverStrategy(DriverStrategy.SORTED_GROUP_COMBINE);
                combineConfig.setDriverComparator(compFact, 0);
                combineConfig.setDriverComparator(compFact, 1);
                combineConfig.setRelativeMemoryDriver(memoryFraction);

                // udf
                combineConfig.setStubWrapper(
                        new UserCodeClassWrapper<>(MockFailingCombineStub.class));

                getTaskConfig()
                        .addChainedTask(
                                SynchronousChainedCombineDriver.class, combineConfig, "combine");
            }

            // chained map+combine
            {
                registerTask(FlatMapDriver.class, MockMapStub.class);
                final BatchTask<FlatMapFunction<Record, Record>, Record> testTask =
                        new BatchTask<>(this.mockEnv);

                boolean stubFailed = false;
                try {
                    testTask.invoke();
                } catch (Exception e) {
                    stubFailed = true;
                }

                Assert.assertTrue("Function exception was not forwarded.", stubFailed);
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testBatchTaskOutputInCloseMethod() {
        final int numChainedTasks = 10;
        final int keyCnt = 100;
        final int valCnt = 10;
        try {
            initEnvironment(MEMORY_MANAGER_SIZE, NETWORK_BUFFER_SIZE);
            addInput(new UniformRecordGenerator(keyCnt, valCnt, false), 0);
            addOutput(outList);
            registerTask(FlatMapDriver.class, MockMapStub.class);
            for (int i = 0; i < numChainedTasks; i++) {
                final TaskConfig taskConfig = new TaskConfig(new Configuration());
                taskConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
                taskConfig.setOutputSerializer(serFact);
                taskConfig.setStubWrapper(
                        new UserCodeClassWrapper<>(MockDuplicateLastValueMapFunction.class));
                getTaskConfig()
                        .addChainedTask(ChainedFlatMapDriver.class, taskConfig, "chained-" + i);
            }
            final BatchTask<FlatMapFunction<Record, Record>, Record> testTask =
                    new BatchTask<>(mockEnv);
            testTask.invoke();
            Assert.assertEquals(keyCnt * valCnt + numChainedTasks, outList.size());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testDataSourceTaskOutputInCloseMethod() throws IOException {
        final int numChainedTasks = 10;
        final int keyCnt = 100;
        final int valCnt = 10;
        final File tempTestFile = new File(tempFolder.getRoot(), UUID.randomUUID().toString());
        DataSourceTaskTest.InputFilePreparator.prepareInputFile(
                new UniformRecordGenerator(keyCnt, valCnt, false), tempTestFile, true);
        initEnvironment(MEMORY_MANAGER_SIZE, NETWORK_BUFFER_SIZE);
        addOutput(outList);
        final DataSourceTask<Record> testTask = new DataSourceTask<>(mockEnv);
        registerFileInputTask(
                testTask,
                DataSourceTaskTest.MockInputFormat.class,
                tempTestFile.toURI().toString(),
                "\n");
        for (int i = 0; i < numChainedTasks; i++) {
            final TaskConfig taskConfig = new TaskConfig(new Configuration());
            taskConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
            taskConfig.setOutputSerializer(serFact);
            taskConfig.setStubWrapper(
                    new UserCodeClassWrapper<>(
                            ChainTaskTest.MockDuplicateLastValueMapFunction.class));
            getTaskConfig().addChainedTask(ChainedFlatMapDriver.class, taskConfig, "chained-" + i);
        }
        try {
            testTask.invoke();
            Assert.assertEquals(keyCnt * valCnt + numChainedTasks, outList.size());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Invoke method caused exception.");
        }
    }

    public static final class MockFailingCombineStub
            implements GroupReduceFunction<Record, Record>, GroupCombineFunction<Record, Record> {
        private static final long serialVersionUID = 1L;

        private int cnt = 0;

        @Override
        public void reduce(Iterable<Record> records, Collector<Record> out) throws Exception {
            if (++this.cnt >= 5) {
                throw new RuntimeException("Expected Test Exception");
            }

            for (Record r : records) {
                out.collect(r);
            }
        }

        @Override
        public void combine(Iterable<Record> values, Collector<Record> out) throws Exception {
            reduce(values, out);
        }
    }

    /**
     * FlatMap function that outputs the last emitted element when closing.
     *
     * @param <T> Input and output type.
     */
    public static class MockDuplicateLastValueMapFunction<T> extends RichFlatMapFunction<T, T> {

        private boolean closed = false;

        private transient T value;
        private transient Collector<T> out;

        @Override
        public void flatMap(T value, Collector<T> out) throws Exception {
            if (closed) {
                // Make sure we close chained task in proper order.
                throw new IllegalStateException("Task is already closed.");
            }
            this.value = value;
            this.out = out;
            out.collect(value);
        }

        @Override
        public void close() throws Exception {
            closed = true;
            out.collect(value);
        }
    }
}
