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

package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.runtime.operators.sort.ExternalSorter;
import org.apache.flink.runtime.operators.sort.Sorter;
import org.apache.flink.runtime.operators.testutils.DelayingInfinitiveInputIterator;
import org.apache.flink.runtime.operators.testutils.DriverTestBase;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.runtime.operators.testutils.NirvanaOutputList;
import org.apache.flink.runtime.operators.testutils.TaskCancelThread;
import org.apache.flink.runtime.operators.testutils.UniformRecordGenerator;
import org.apache.flink.runtime.testutils.recordutils.RecordComparator;
import org.apache.flink.runtime.testutils.recordutils.RecordSerializerFactory;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.Value;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

public class ReduceTaskTest extends DriverTestBase<RichGroupReduceFunction<Record, Record>> {
    private static final Logger LOG = LoggerFactory.getLogger(ReduceTaskTest.class);

    @SuppressWarnings("unchecked")
    private final RecordComparator comparator =
            new RecordComparator(
                    new int[] {0}, (Class<? extends Value>[]) new Class[] {IntValue.class});

    private final List<Record> outList = new ArrayList<>();

    public ReduceTaskTest(ExecutionConfig config) {
        super(config, 0, 1, 3 * 1024 * 1024);
    }

    @TestTemplate
    void testReduceTaskWithSortingInput() {
        final int keyCnt = 100;
        final int valCnt = 20;

        addDriverComparator(this.comparator);
        setOutput(this.outList);
        getTaskConfig().setDriverStrategy(DriverStrategy.SORTED_GROUP_REDUCE);

        try {
            addInputSorted(
                    new UniformRecordGenerator(keyCnt, valCnt, false), this.comparator.duplicate());

            GroupReduceDriver<Record, Record> testTask = new GroupReduceDriver<>();

            testDriver(testTask, MockReduceStub.class);
        } catch (Exception e) {
            LOG.info("Exception while running the test task.", e);
            fail("Exception in Test: " + e.getMessage());
        }

        assertThat(this.outList)
                .withFailMessage("Resultset size was %d. Expected was %d", outList.size(), keyCnt)
                .hasSize(keyCnt);

        for (Record record : this.outList) {
            assertThat(record.getField(1, IntValue.class).getValue())
                    .withFailMessage("Incorrect result")
                    .isEqualTo(valCnt - record.getField(0, IntValue.class).getValue());
        }

        this.outList.clear();
    }

    @TestTemplate
    void testReduceTaskOnPreSortedInput() {
        final int keyCnt = 100;
        final int valCnt = 20;

        addInput(new UniformRecordGenerator(keyCnt, valCnt, true));
        addDriverComparator(this.comparator);
        setOutput(this.outList);
        getTaskConfig().setDriverStrategy(DriverStrategy.SORTED_GROUP_REDUCE);

        GroupReduceDriver<Record, Record> testTask = new GroupReduceDriver<>();

        try {
            testDriver(testTask, MockReduceStub.class);
        } catch (Exception e) {
            LOG.info("Exception while running the test task.", e);
            fail("Invoke method caused exception: " + e.getMessage());
        }

        assertThat(this.outList)
                .withFailMessage("Resultset size was %d. Expected was %d", outList.size(), keyCnt)
                .hasSize(keyCnt);

        for (Record record : this.outList) {
            assertThat(record.getField(1, IntValue.class).getValue())
                    .withFailMessage("Incorrect result")
                    .isEqualTo(valCnt - record.getField(0, IntValue.class).getValue());
        }

        this.outList.clear();
    }

    @TestTemplate
    void testCombiningReduceTask() throws IOException {
        final int keyCnt = 100;
        final int valCnt = 20;

        addDriverComparator(this.comparator);
        setOutput(this.outList);
        getTaskConfig().setDriverStrategy(DriverStrategy.SORTED_GROUP_REDUCE);

        Sorter<Record> sorter = null;
        try {
            sorter =
                    ExternalSorter.newBuilder(
                                    getMemoryManager(),
                                    getContainingTask(),
                                    RecordSerializerFactory.get().getSerializer(),
                                    this.comparator.duplicate())
                            .maxNumFileHandles(4)
                            .withCombiner(new ReduceTaskExternalITCase.MockCombiningReduceStub())
                            .enableSpilling(getIOManager(), 0.8f)
                            .memoryFraction(this.perSortFractionMem)
                            .objectReuse(true)
                            .largeRecords(true)
                            .build(new UniformRecordGenerator(keyCnt, valCnt, false));

            addInput(sorter.getIterator());

            GroupReduceDriver<Record, Record> testTask = new GroupReduceDriver<>();

            testDriver(testTask, MockCombiningReduceStub.class);
        } catch (Exception e) {
            LOG.info("Exception while running the test task.", e);
            fail("Invoke method caused exception: " + e.getMessage());
        } finally {
            if (sorter != null) {
                sorter.close();
            }
        }

        int expSum = 0;
        for (int i = 1; i < valCnt; i++) {
            expSum += i;
        }

        assertThat(this.outList)
                .withFailMessage("Resultset size was %d. Expected was %d", outList.size(), keyCnt)
                .hasSize(keyCnt);

        for (Record record : this.outList) {
            assertThat(record.getField(1, IntValue.class).getValue())
                    .withFailMessage("Incorrect result")
                    .isEqualTo(expSum - record.getField(0, IntValue.class).getValue());
        }

        this.outList.clear();
    }

    @TestTemplate
    void testFailingReduceTask() {
        final int keyCnt = 100;
        final int valCnt = 20;

        addInput(new UniformRecordGenerator(keyCnt, valCnt, true));
        addDriverComparator(this.comparator);
        setOutput(this.outList);
        getTaskConfig().setDriverStrategy(DriverStrategy.SORTED_GROUP_REDUCE);

        GroupReduceDriver<Record, Record> testTask = new GroupReduceDriver<>();

        assertThatThrownBy(() -> testDriver(testTask, MockFailingReduceStub.class))
                .isInstanceOf(ExpectedTestException.class);

        this.outList.clear();
    }

    @TestTemplate
    void testCancelReduceTaskWhileSorting() {
        addDriverComparator(this.comparator);
        setOutput(new NirvanaOutputList());
        getTaskConfig().setDriverStrategy(DriverStrategy.SORTED_GROUP_REDUCE);

        final GroupReduceDriver<Record, Record> testTask = new GroupReduceDriver<>();

        try {
            addInputSorted(new DelayingInfinitiveInputIterator(100), this.comparator.duplicate());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        final AtomicBoolean success = new AtomicBoolean(false);

        Thread taskRunner =
                new Thread() {
                    @Override
                    public void run() {
                        try {
                            testDriver(testTask, MockReduceStub.class);
                            success.set(true);
                        } catch (Exception ie) {
                            ie.printStackTrace();
                        }
                    }
                };
        taskRunner.start();

        TaskCancelThread tct = new TaskCancelThread(1, taskRunner, this);
        tct.start();

        try {
            tct.join();
            taskRunner.join();
        } catch (InterruptedException ie) {
            fail("Joining threads failed");
        }

        assertThat(success)
                .withFailMessage("Test threw an exception even though it was properly canceled.")
                .isTrue();
    }

    @TestTemplate
    void testCancelReduceTaskWhileReducing() {

        final int keyCnt = 1000;
        final int valCnt = 2;

        addInput(new UniformRecordGenerator(keyCnt, valCnt, true));
        addDriverComparator(this.comparator);
        setOutput(new NirvanaOutputList());
        getTaskConfig().setDriverStrategy(DriverStrategy.SORTED_GROUP_REDUCE);

        final GroupReduceDriver<Record, Record> testTask = new GroupReduceDriver<>();

        final AtomicBoolean success = new AtomicBoolean(false);

        Thread taskRunner =
                new Thread() {
                    @Override
                    public void run() {
                        try {
                            testDriver(testTask, MockDelayingReduceStub.class);
                            success.set(true);
                        } catch (Exception ie) {
                            ie.printStackTrace();
                        }
                    }
                };
        taskRunner.start();

        TaskCancelThread tct = new TaskCancelThread(2, taskRunner, this);
        tct.start();

        try {
            tct.join();
            taskRunner.join();
        } catch (InterruptedException ie) {
            fail("Joining threads failed");
        }
    }

    public static class MockReduceStub extends RichGroupReduceFunction<Record, Record> {
        private static final long serialVersionUID = 1L;

        private final IntValue key = new IntValue();
        private final IntValue value = new IntValue();

        @Override
        public void reduce(Iterable<Record> records, Collector<Record> out) {
            Record element = null;
            int cnt = 0;

            for (Record next : records) {
                element = next;
                cnt++;
            }
            element.getField(0, this.key);
            this.value.setValue(cnt - this.key.getValue());
            element.setField(1, this.value);
            out.collect(element);
        }
    }

    public static class MockCombiningReduceStub
            implements GroupReduceFunction<Record, Record>, GroupCombineFunction<Record, Record> {

        private static final long serialVersionUID = 1L;

        private final IntValue key = new IntValue();
        private final IntValue value = new IntValue();
        private final IntValue combineValue = new IntValue();

        @Override
        public void reduce(Iterable<Record> records, Collector<Record> out) {
            Record element = null;
            int sum = 0;

            for (Record next : records) {
                element = next;
                element.getField(1, this.value);

                sum += this.value.getValue();
            }
            element.getField(0, this.key);
            this.value.setValue(sum - this.key.getValue());
            element.setField(1, this.value);
            out.collect(element);
        }

        @Override
        public void combine(Iterable<Record> records, Collector<Record> out) {
            Record element = null;
            int sum = 0;

            for (Record next : records) {
                element = next;
                element.getField(1, this.combineValue);

                sum += this.combineValue.getValue();
            }

            this.combineValue.setValue(sum);
            element.setField(1, this.combineValue);
            out.collect(element);
        }
    }

    public static class MockFailingReduceStub extends RichGroupReduceFunction<Record, Record> {
        private static final long serialVersionUID = 1L;

        private int cnt = 0;

        private final IntValue key = new IntValue();
        private final IntValue value = new IntValue();

        @Override
        public void reduce(Iterable<Record> records, Collector<Record> out) {
            Record element = null;
            int valCnt = 0;

            for (Record next : records) {
                element = next;
                valCnt++;
            }

            if (++this.cnt >= 10) {
                throw new ExpectedTestException();
            }

            element.getField(0, this.key);
            this.value.setValue(valCnt - this.key.getValue());
            element.setField(1, this.value);
            out.collect(element);
        }
    }

    public static class MockDelayingReduceStub extends RichGroupReduceFunction<Record, Record> {
        private static final long serialVersionUID = 1L;

        @Override
        public void reduce(Iterable<Record> records, Collector<Record> out) {
            for (@SuppressWarnings("unused") Record r : records) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }
            }
        }
    }
}
