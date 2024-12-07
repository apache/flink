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
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.operators.CoGroupTaskExternalITCase.MockCoGroupStub;
import org.apache.flink.runtime.operators.testutils.DelayingInfinitiveInputIterator;
import org.apache.flink.runtime.operators.testutils.DriverTestBase;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.runtime.operators.testutils.TaskCancelThread;
import org.apache.flink.runtime.operators.testutils.UniformRecordGenerator;
import org.apache.flink.runtime.testutils.recordutils.RecordComparator;
import org.apache.flink.runtime.testutils.recordutils.RecordPairComparatorFactory;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.Value;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.TestTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CoGroupTaskTest extends DriverTestBase<CoGroupFunction<Record, Record, Record>> {
    private static final long SORT_MEM = 3 * 1024 * 1024;

    @SuppressWarnings("unchecked")
    private final RecordComparator comparator1 =
            new RecordComparator(
                    new int[] {0}, (Class<? extends Value>[]) new Class[] {IntValue.class});

    @SuppressWarnings("unchecked")
    private final RecordComparator comparator2 =
            new RecordComparator(
                    new int[] {0}, (Class<? extends Value>[]) new Class[] {IntValue.class});

    private final CountingOutputCollector output = new CountingOutputCollector();

    CoGroupTaskTest(ExecutionConfig config) {
        super(config, 0, 2, SORT_MEM);
    }

    @TestTemplate
    void testSortBoth1CoGroupTask() throws Exception {
        int keyCnt1 = 100;
        int valCnt1 = 2;

        int keyCnt2 = 200;
        int valCnt2 = 1;

        final int expCnt =
                valCnt1 * valCnt2 * Math.min(keyCnt1, keyCnt2)
                        + (keyCnt1 > keyCnt2
                                ? (keyCnt1 - keyCnt2) * valCnt1
                                : (keyCnt2 - keyCnt1) * valCnt2);

        setOutput(this.output);
        addDriverComparator(this.comparator1);
        addDriverComparator(this.comparator2);
        getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
        getTaskConfig().setDriverStrategy(DriverStrategy.CO_GROUP);

        final CoGroupDriver<Record, Record, Record> testTask = new CoGroupDriver<>();

        addInputSorted(
                new UniformRecordGenerator(keyCnt1, valCnt1, false), this.comparator1.duplicate());
        addInputSorted(
                new UniformRecordGenerator(keyCnt2, valCnt2, false), this.comparator2.duplicate());
        testDriver(testTask, MockCoGroupStub.class);

        assertThat(this.output.getNumberOfRecords())
                .withFailMessage("Wrong result set size.")
                .isEqualTo(expCnt);
    }

    @TestTemplate
    void testSortBoth2CoGroupTask() throws Exception {
        int keyCnt1 = 200;
        int valCnt1 = 2;

        int keyCnt2 = 200;
        int valCnt2 = 4;

        final int expCnt =
                valCnt1 * valCnt2 * Math.min(keyCnt1, keyCnt2)
                        + (keyCnt1 > keyCnt2
                                ? (keyCnt1 - keyCnt2) * valCnt1
                                : (keyCnt2 - keyCnt1) * valCnt2);

        setOutput(this.output);
        addDriverComparator(this.comparator1);
        addDriverComparator(this.comparator2);
        getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
        getTaskConfig().setDriverStrategy(DriverStrategy.CO_GROUP);

        final CoGroupDriver<Record, Record, Record> testTask = new CoGroupDriver<>();

        addInputSorted(
                new UniformRecordGenerator(keyCnt1, valCnt1, false), this.comparator1.duplicate());
        addInputSorted(
                new UniformRecordGenerator(keyCnt2, valCnt2, false), this.comparator2.duplicate());
        testDriver(testTask, MockCoGroupStub.class);

        assertThat(this.output.getNumberOfRecords())
                .withFailMessage("Wrong result set size.")
                .isEqualTo(expCnt);
    }

    @TestTemplate
    void testSortFirstCoGroupTask() throws Exception {
        int keyCnt1 = 200;
        int valCnt1 = 2;

        int keyCnt2 = 200;
        int valCnt2 = 4;

        final int expCnt =
                valCnt1 * valCnt2 * Math.min(keyCnt1, keyCnt2)
                        + (keyCnt1 > keyCnt2
                                ? (keyCnt1 - keyCnt2) * valCnt1
                                : (keyCnt2 - keyCnt1) * valCnt2);

        setOutput(this.output);
        addDriverComparator(this.comparator1);
        addDriverComparator(this.comparator2);
        getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
        getTaskConfig().setDriverStrategy(DriverStrategy.CO_GROUP);

        final CoGroupDriver<Record, Record, Record> testTask = new CoGroupDriver<>();

        addInputSorted(
                new UniformRecordGenerator(keyCnt1, valCnt1, false), this.comparator1.duplicate());
        addInput(new UniformRecordGenerator(keyCnt2, valCnt2, true));
        testDriver(testTask, MockCoGroupStub.class);

        assertThat(this.output.getNumberOfRecords())
                .withFailMessage("Wrong result set size.")
                .isEqualTo(expCnt);
    }

    @TestTemplate
    void testSortSecondCoGroupTask() throws Exception {
        int keyCnt1 = 200;
        int valCnt1 = 2;

        int keyCnt2 = 200;
        int valCnt2 = 4;

        final int expCnt =
                valCnt1 * valCnt2 * Math.min(keyCnt1, keyCnt2)
                        + (keyCnt1 > keyCnt2
                                ? (keyCnt1 - keyCnt2) * valCnt1
                                : (keyCnt2 - keyCnt1) * valCnt2);

        setOutput(this.output);
        addDriverComparator(this.comparator1);
        addDriverComparator(this.comparator2);
        getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
        getTaskConfig().setDriverStrategy(DriverStrategy.CO_GROUP);

        final CoGroupDriver<Record, Record, Record> testTask = new CoGroupDriver<>();

        addInput(new UniformRecordGenerator(keyCnt1, valCnt1, true));
        addInputSorted(
                new UniformRecordGenerator(keyCnt2, valCnt2, false), this.comparator2.duplicate());
        testDriver(testTask, MockCoGroupStub.class);

        assertThat(this.output.getNumberOfRecords())
                .withFailMessage("Wrong result set size.")
                .isEqualTo(expCnt);
    }

    @TestTemplate
    void testMergeCoGroupTask() throws Exception {
        int keyCnt1 = 200;
        int valCnt1 = 2;

        int keyCnt2 = 200;
        int valCnt2 = 4;

        final int expCnt =
                valCnt1 * valCnt2 * Math.min(keyCnt1, keyCnt2)
                        + (keyCnt1 > keyCnt2
                                ? (keyCnt1 - keyCnt2) * valCnt1
                                : (keyCnt2 - keyCnt1) * valCnt2);

        setOutput(this.output);

        addInput(new UniformRecordGenerator(keyCnt1, valCnt1, true));
        addInput(new UniformRecordGenerator(keyCnt2, valCnt2, true));
        addDriverComparator(this.comparator1);
        addDriverComparator(this.comparator2);

        getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
        getTaskConfig().setDriverStrategy(DriverStrategy.CO_GROUP);

        final CoGroupDriver<Record, Record, Record> testTask = new CoGroupDriver<>();

        testDriver(testTask, MockCoGroupStub.class);

        assertThat(this.output.getNumberOfRecords())
                .withFailMessage("Wrong result set size.")
                .isEqualTo(expCnt);
    }

    @TestTemplate
    void testFailingSortCoGroupTask() {
        int keyCnt1 = 100;
        int valCnt1 = 2;

        int keyCnt2 = 200;
        int valCnt2 = 1;

        setOutput(this.output);

        addInput(new UniformRecordGenerator(keyCnt1, valCnt1, true));
        addInput(new UniformRecordGenerator(keyCnt2, valCnt2, true));
        addDriverComparator(this.comparator1);
        addDriverComparator(this.comparator2);

        getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
        getTaskConfig().setDriverStrategy(DriverStrategy.CO_GROUP);

        final CoGroupDriver<Record, Record, Record> testTask = new CoGroupDriver<>();

        assertThatThrownBy(() -> testDriver(testTask, MockFailingCoGroupStub.class))
                .isInstanceOf(ExpectedTestException.class);
    }

    @TestTemplate
    void testCancelCoGroupTaskWhileSorting1() throws Exception {
        int keyCnt = 10;
        int valCnt = 2;

        setOutput(this.output);

        addDriverComparator(this.comparator1);
        addDriverComparator(this.comparator2);

        getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
        getTaskConfig().setDriverStrategy(DriverStrategy.CO_GROUP);

        final CoGroupDriver<Record, Record, Record> testTask = new CoGroupDriver<>();

        addInputSorted(new DelayingInfinitiveInputIterator(1000), this.comparator1.duplicate());
        addInput(new UniformRecordGenerator(keyCnt, valCnt, true));

        CheckedThread taskRunner =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        testDriver(testTask, MockCoGroupStub.class);
                    }
                };
        taskRunner.start();

        TaskCancelThread tct = new TaskCancelThread(1, taskRunner, this);
        tct.start();

        tct.join();
        taskRunner.sync();
    }

    @TestTemplate
    void testCancelCoGroupTaskWhileSorting2() throws Exception {
        int keyCnt = 10;
        int valCnt = 2;

        setOutput(this.output);

        addDriverComparator(this.comparator1);
        addDriverComparator(this.comparator2);

        getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
        getTaskConfig().setDriverStrategy(DriverStrategy.CO_GROUP);

        final CoGroupDriver<Record, Record, Record> testTask = new CoGroupDriver<>();

        addInput(new UniformRecordGenerator(keyCnt, valCnt, true));
        addInputSorted(new DelayingInfinitiveInputIterator(1000), this.comparator2.duplicate());

        CheckedThread taskRunner =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        testDriver(testTask, MockCoGroupStub.class);
                    }
                };
        taskRunner.start();

        TaskCancelThread tct = new TaskCancelThread(1, taskRunner, this);
        tct.start();

        tct.join();
        taskRunner.sync();
    }

    @TestTemplate
    void testCancelCoGroupTaskWhileCoGrouping() throws Exception {
        int keyCnt = 100;
        int valCnt = 5;

        setOutput(this.output);

        addDriverComparator(this.comparator1);
        addDriverComparator(this.comparator2);

        getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
        getTaskConfig().setDriverStrategy(DriverStrategy.CO_GROUP);

        final CoGroupDriver<Record, Record, Record> testTask = new CoGroupDriver<>();

        addInput(new UniformRecordGenerator(keyCnt, valCnt, true));
        addInput(new UniformRecordGenerator(keyCnt, valCnt, true));

        final OneShotLatch delayCoGroupProcessingLatch = new OneShotLatch();

        CheckedThread taskRunner =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        testDriver(
                                testTask, new MockDelayingCoGroupStub(delayCoGroupProcessingLatch));
                    }
                };
        taskRunner.start();

        TaskCancelThread tct = new TaskCancelThread(1, taskRunner, this);
        tct.start();

        tct.join();
        delayCoGroupProcessingLatch.trigger();
        taskRunner.sync();
    }

    public static class MockFailingCoGroupStub extends RichCoGroupFunction<Record, Record, Record> {
        private static final long serialVersionUID = 1L;

        private int cnt = 0;

        @Override
        public void coGroup(
                Iterable<Record> records1, Iterable<Record> records2, Collector<Record> out) {
            int val1Cnt = 0;

            for (@SuppressWarnings("unused") Record r : records1) {
                val1Cnt++;
            }

            for (Record record2 : records2) {
                if (val1Cnt == 0) {

                    if (++this.cnt >= 10) {
                        throw new ExpectedTestException();
                    }

                    out.collect(record2);
                } else {
                    for (int i = 0; i < val1Cnt; i++) {

                        if (++this.cnt >= 10) {
                            throw new ExpectedTestException();
                        }

                        out.collect(record2);
                    }
                }
            }
        }
    }

    public static final class MockDelayingCoGroupStub
            extends RichCoGroupFunction<Record, Record, Record> {
        private static final long serialVersionUID = 1L;

        private final OneShotLatch delayCoGroupProcessingLatch;

        public MockDelayingCoGroupStub(OneShotLatch delayCoGroupProcessingLatch) {
            this.delayCoGroupProcessingLatch = delayCoGroupProcessingLatch;
        }

        @Override
        public void coGroup(
                Iterable<Record> records1, Iterable<Record> records2, Collector<Record> out)
                throws InterruptedException {
            delayCoGroupProcessingLatch.await();
        }
    }
}
