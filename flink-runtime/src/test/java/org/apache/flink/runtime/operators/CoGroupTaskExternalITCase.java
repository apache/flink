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
import org.apache.flink.runtime.operators.testutils.DriverTestBase;
import org.apache.flink.runtime.operators.testutils.UniformRecordGenerator;
import org.apache.flink.runtime.testutils.recordutils.RecordComparator;
import org.apache.flink.runtime.testutils.recordutils.RecordPairComparatorFactory;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.Value;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;

public class CoGroupTaskExternalITCase
        extends DriverTestBase<CoGroupFunction<Record, Record, Record>> {
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

    public CoGroupTaskExternalITCase(ExecutionConfig config) {
        super(config, 0, 2, SORT_MEM);
    }

    @Test
    public void testExternalSortCoGroupTask() {

        int keyCnt1 = 16384 * 8;
        int valCnt1 = 32;

        int keyCnt2 = 65536 * 4;
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

        final CoGroupDriver<Record, Record, Record> testTask =
                new CoGroupDriver<Record, Record, Record>();

        try {
            addInputSorted(
                    new UniformRecordGenerator(keyCnt1, valCnt1, false),
                    this.comparator1.duplicate());
            addInputSorted(
                    new UniformRecordGenerator(keyCnt2, valCnt2, false),
                    this.comparator2.duplicate());
            testDriver(testTask, MockCoGroupStub.class);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("The test caused an exception.");
        }

        Assert.assertEquals("Wrong result set size.", expCnt, this.output.getNumberOfRecords());
    }

    public static final class MockCoGroupStub extends RichCoGroupFunction<Record, Record, Record> {
        private static final long serialVersionUID = 1L;

        private final Record res = new Record();

        @SuppressWarnings("unused")
        @Override
        public void coGroup(
                Iterable<Record> records1, Iterable<Record> records2, Collector<Record> out) {
            int val1Cnt = 0;
            int val2Cnt = 0;

            for (Record r : records1) {
                val1Cnt++;
            }

            for (Record r : records2) {
                val2Cnt++;
            }

            if (val1Cnt == 0) {
                for (int i = 0; i < val2Cnt; i++) {
                    out.collect(this.res);
                }
            } else if (val2Cnt == 0) {
                for (int i = 0; i < val1Cnt; i++) {
                    out.collect(this.res);
                }
            } else {
                for (int i = 0; i < val2Cnt * val1Cnt; i++) {
                    out.collect(this.res);
                }
            }
        }
    }
}
