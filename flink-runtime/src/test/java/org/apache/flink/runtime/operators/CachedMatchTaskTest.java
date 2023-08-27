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
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.runtime.operators.testutils.DelayingInfinitiveInputIterator;
import org.apache.flink.runtime.operators.testutils.DriverTestBase;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.runtime.operators.testutils.NirvanaOutputList;
import org.apache.flink.runtime.operators.testutils.TaskCancelThread;
import org.apache.flink.runtime.operators.testutils.UniformRecordGenerator;
import org.apache.flink.runtime.testutils.recordutils.RecordComparator;
import org.apache.flink.runtime.testutils.recordutils.RecordPairComparatorFactory;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.Value;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.TestTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

public class CachedMatchTaskTest extends DriverTestBase<FlatJoinFunction<Record, Record, Record>> {

    private static final long HASH_MEM = 6 * 1024 * 1024;

    private static final long SORT_MEM = 3 * 1024 * 1024;

    @SuppressWarnings("unchecked")
    private final RecordComparator comparator1 =
            new RecordComparator(
                    new int[] {0}, (Class<? extends Value>[]) new Class[] {IntValue.class});

    @SuppressWarnings("unchecked")
    private final RecordComparator comparator2 =
            new RecordComparator(
                    new int[] {0}, (Class<? extends Value>[]) new Class[] {IntValue.class});

    private final List<Record> outList = new ArrayList<Record>();

    public CachedMatchTaskTest(ExecutionConfig config) {
        super(config, HASH_MEM, 2, SORT_MEM);
    }

    @TestTemplate
    void testHash1MatchTask() {
        int keyCnt1 = 20;
        int valCnt1 = 1;

        int keyCnt2 = 10;
        int valCnt2 = 2;

        addInput(new UniformRecordGenerator(keyCnt1, valCnt1, false));
        addInput(new UniformRecordGenerator(keyCnt2, valCnt2, false));
        addDriverComparator(this.comparator1);
        addDriverComparator(this.comparator2);
        getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
        setOutput(this.outList);
        getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_FIRST_CACHED);
        getTaskConfig().setRelativeMemoryDriver(1.0f);

        BuildFirstCachedJoinDriver<Record, Record, Record> testTask =
                new BuildFirstCachedJoinDriver<Record, Record, Record>();

        try {
            testResettableDriver(testTask, MockMatchStub.class, 3);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Test caused an exception.");
        }

        final int expCnt = valCnt1 * valCnt2 * Math.min(keyCnt1, keyCnt2);
        assertThat(this.outList).withFailMessage("Wrong result set size.").hasSize(expCnt);
        this.outList.clear();
    }

    @TestTemplate
    void testHash2MatchTask() {
        int keyCnt1 = 20;
        int valCnt1 = 1;

        int keyCnt2 = 20;
        int valCnt2 = 1;

        addInput(new UniformRecordGenerator(keyCnt1, valCnt1, false));
        addInput(new UniformRecordGenerator(keyCnt2, valCnt2, false));
        addDriverComparator(this.comparator1);
        addDriverComparator(this.comparator2);
        getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
        setOutput(this.outList);
        getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_SECOND_CACHED);
        getTaskConfig().setRelativeMemoryDriver(1.0f);

        BuildSecondCachedJoinDriver<Record, Record, Record> testTask =
                new BuildSecondCachedJoinDriver<Record, Record, Record>();

        try {
            testResettableDriver(testTask, MockMatchStub.class, 3);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Test caused an exception.");
        }

        final int expCnt = valCnt1 * valCnt2 * Math.min(keyCnt1, keyCnt2);
        assertThat(this.outList).withFailMessage("Wrong result set size.").hasSize(expCnt);
        this.outList.clear();
    }

    @TestTemplate
    void testHash3MatchTask() {
        int keyCnt1 = 20;
        int valCnt1 = 1;

        int keyCnt2 = 20;
        int valCnt2 = 20;

        addInput(new UniformRecordGenerator(keyCnt1, valCnt1, false));
        addInput(new UniformRecordGenerator(keyCnt2, valCnt2, false));
        addDriverComparator(this.comparator1);
        addDriverComparator(this.comparator2);
        getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
        setOutput(this.outList);
        getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_FIRST_CACHED);
        getTaskConfig().setRelativeMemoryDriver(1.0f);

        BuildFirstCachedJoinDriver<Record, Record, Record> testTask =
                new BuildFirstCachedJoinDriver<Record, Record, Record>();

        try {
            testResettableDriver(testTask, MockMatchStub.class, 3);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Test caused an exception.");
        }

        final int expCnt = valCnt1 * valCnt2 * Math.min(keyCnt1, keyCnt2);
        assertThat(this.outList).withFailMessage("Wrong result set size.").hasSize(expCnt);
        this.outList.clear();
    }

    @TestTemplate
    void testHash4MatchTask() {
        int keyCnt1 = 20;
        int valCnt1 = 20;

        int keyCnt2 = 20;
        int valCnt2 = 1;

        addInput(new UniformRecordGenerator(keyCnt1, valCnt1, false));
        addInput(new UniformRecordGenerator(keyCnt2, valCnt2, false));
        addDriverComparator(this.comparator1);
        addDriverComparator(this.comparator2);
        getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
        setOutput(this.outList);
        getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_SECOND_CACHED);
        getTaskConfig().setRelativeMemoryDriver(1.0f);

        BuildSecondCachedJoinDriver<Record, Record, Record> testTask =
                new BuildSecondCachedJoinDriver<Record, Record, Record>();

        try {
            testResettableDriver(testTask, MockMatchStub.class, 3);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Test caused an exception.");
        }

        final int expCnt = valCnt1 * valCnt2 * Math.min(keyCnt1, keyCnt2);
        assertThat(this.outList).withFailMessage("Wrong result set size.").hasSize(expCnt);
        this.outList.clear();
    }

    @TestTemplate
    void testHash5MatchTask() {
        int keyCnt1 = 20;
        int valCnt1 = 20;

        int keyCnt2 = 20;
        int valCnt2 = 20;

        addInput(new UniformRecordGenerator(keyCnt1, valCnt1, false));
        addInput(new UniformRecordGenerator(keyCnt2, valCnt2, false));
        addDriverComparator(this.comparator1);
        addDriverComparator(this.comparator2);
        getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
        setOutput(this.outList);
        getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_FIRST_CACHED);
        getTaskConfig().setRelativeMemoryDriver(1.0f);

        BuildFirstCachedJoinDriver<Record, Record, Record> testTask =
                new BuildFirstCachedJoinDriver<Record, Record, Record>();

        try {
            testResettableDriver(testTask, MockMatchStub.class, 3);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Test caused an exception.");
        }

        final int expCnt = valCnt1 * valCnt2 * Math.min(keyCnt1, keyCnt2);
        assertThat(this.outList).withFailMessage("Wrong result set size.").hasSize(expCnt);
        this.outList.clear();
    }

    @TestTemplate
    void testFailingHashFirstMatchTask() {
        int keyCnt1 = 20;
        int valCnt1 = 20;

        int keyCnt2 = 20;
        int valCnt2 = 20;

        addInput(new UniformRecordGenerator(keyCnt1, valCnt1, false));
        addInput(new UniformRecordGenerator(keyCnt2, valCnt2, false));
        addDriverComparator(this.comparator1);
        addDriverComparator(this.comparator2);
        getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
        setOutput(new NirvanaOutputList());
        getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_FIRST_CACHED);
        getTaskConfig().setRelativeMemoryDriver(1.0f);

        BuildFirstCachedJoinDriver<Record, Record, Record> testTask =
                new BuildFirstCachedJoinDriver<Record, Record, Record>();

        assertThatThrownBy(() -> testResettableDriver(testTask, MockFailingMatchStub.class, 3))
                .isInstanceOf(ExpectedTestException.class);
    }

    @TestTemplate
    void testFailingHashSecondMatchTask() {
        int keyCnt1 = 20;
        int valCnt1 = 20;

        int keyCnt2 = 20;
        int valCnt2 = 20;

        addInput(new UniformRecordGenerator(keyCnt1, valCnt1, false));
        addInput(new UniformRecordGenerator(keyCnt2, valCnt2, false));
        addDriverComparator(this.comparator1);
        addDriverComparator(this.comparator2);
        getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
        setOutput(new NirvanaOutputList());
        getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_SECOND_CACHED);
        getTaskConfig().setRelativeMemoryDriver(1.0f);

        BuildSecondCachedJoinDriver<Record, Record, Record> testTask =
                new BuildSecondCachedJoinDriver<Record, Record, Record>();

        assertThatThrownBy(() -> testResettableDriver(testTask, MockFailingMatchStub.class, 3))
                .isInstanceOf(ExpectedTestException.class);
    }

    @TestTemplate
    void testCancelHashMatchTaskWhileBuildFirst() {
        int keyCnt = 20;
        int valCnt = 20;

        addInput(new DelayingInfinitiveInputIterator(100));
        addInput(new UniformRecordGenerator(keyCnt, valCnt, false));

        addDriverComparator(this.comparator1);
        addDriverComparator(this.comparator2);

        getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());

        setOutput(new NirvanaOutputList());

        getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_FIRST_CACHED);
        getTaskConfig().setRelativeMemoryDriver(1.0f);

        final BuildFirstCachedJoinDriver<Record, Record, Record> testTask =
                new BuildFirstCachedJoinDriver<Record, Record, Record>();

        final AtomicBoolean success = new AtomicBoolean(false);

        Thread taskRunner =
                new Thread() {
                    @Override
                    public void run() {
                        try {
                            testDriver(testTask, MockFailingMatchStub.class);
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
    void testHashCancelMatchTaskWhileBuildSecond() {
        int keyCnt = 20;
        int valCnt = 20;

        addInput(new UniformRecordGenerator(keyCnt, valCnt, false));
        addInput(new DelayingInfinitiveInputIterator(100));
        addDriverComparator(this.comparator1);
        addDriverComparator(this.comparator2);
        getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
        setOutput(new NirvanaOutputList());
        getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_SECOND_CACHED);
        getTaskConfig().setRelativeMemoryDriver(1.0f);

        final BuildSecondCachedJoinDriver<Record, Record, Record> testTask =
                new BuildSecondCachedJoinDriver<Record, Record, Record>();

        final AtomicBoolean success = new AtomicBoolean(false);

        Thread taskRunner =
                new Thread() {
                    @Override
                    public void run() {
                        try {
                            testDriver(testTask, MockMatchStub.class);
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
    void testHashFirstCancelMatchTaskWhileMatching() {
        int keyCnt = 20;
        int valCnt = 20;

        addInput(new UniformRecordGenerator(keyCnt, valCnt, false));
        addInput(new UniformRecordGenerator(keyCnt, valCnt, false));
        addDriverComparator(this.comparator1);
        addDriverComparator(this.comparator2);
        getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
        setOutput(new NirvanaOutputList());
        getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_FIRST);
        getTaskConfig().setRelativeMemoryDriver(1.0f);

        final BuildFirstCachedJoinDriver<Record, Record, Record> testTask =
                new BuildFirstCachedJoinDriver<Record, Record, Record>();

        final AtomicBoolean success = new AtomicBoolean(false);

        Thread taskRunner =
                new Thread() {
                    @Override
                    public void run() {
                        try {
                            testDriver(testTask, MockMatchStub.class);
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
    void testHashSecondCancelMatchTaskWhileMatching() {
        int keyCnt = 20;
        int valCnt = 20;

        addInput(new UniformRecordGenerator(keyCnt, valCnt, false));
        addInput(new UniformRecordGenerator(keyCnt, valCnt, false));
        addDriverComparator(this.comparator1);
        addDriverComparator(this.comparator2);
        getTaskConfig().setDriverPairComparator(RecordPairComparatorFactory.get());
        setOutput(new NirvanaOutputList());
        getTaskConfig().setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_SECOND);
        getTaskConfig().setRelativeMemoryDriver(1.0f);

        final BuildSecondCachedJoinDriver<Record, Record, Record> testTask =
                new BuildSecondCachedJoinDriver<Record, Record, Record>();

        final AtomicBoolean success = new AtomicBoolean(false);

        Thread taskRunner =
                new Thread() {
                    @Override
                    public void run() {
                        try {
                            testDriver(testTask, MockMatchStub.class);
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

    // =================================================================================================

    public static final class MockMatchStub extends RichFlatJoinFunction<Record, Record, Record> {
        private static final long serialVersionUID = 1L;

        @Override
        public void join(Record record1, Record record2, Collector<Record> out) throws Exception {
            out.collect(record1);
        }
    }

    public static final class MockFailingMatchStub
            extends RichFlatJoinFunction<Record, Record, Record> {
        private static final long serialVersionUID = 1L;

        private int cnt = 0;

        @Override
        public void join(Record record1, Record record2, Collector<Record> out) throws Exception {
            if (++this.cnt >= 10) {
                throw new ExpectedTestException();
            }

            out.collect(record1);
        }
    }
}
