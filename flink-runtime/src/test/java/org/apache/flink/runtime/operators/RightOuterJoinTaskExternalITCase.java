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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.RuntimePairComparatorFactory;
import org.apache.flink.runtime.operators.testutils.UniformIntTupleGenerator;

import org.junit.Assert;
import org.junit.Test;

public class RightOuterJoinTaskExternalITCase extends AbstractOuterJoinTaskExternalITCase {

    private final double hash_frac;

    public RightOuterJoinTaskExternalITCase(ExecutionConfig config) {
        super(config);
        hash_frac = (double) HASH_MEM / this.getMemoryManager().getMemorySize();
    }

    @Override
    protected int calculateExpectedCount(int keyCnt1, int valCnt1, int keyCnt2, int valCnt2) {
        return valCnt1 * valCnt2 * Math.min(keyCnt1, keyCnt2)
                + (keyCnt2 > keyCnt1 ? (keyCnt2 - keyCnt1) * valCnt2 : 0);
    }

    @Override
    protected AbstractOuterJoinDriver<
                    Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>
            getOuterJoinDriver() {
        return new RightOuterJoinDriver<>();
    }

    @Override
    protected DriverStrategy getSortStrategy() {
        return DriverStrategy.RIGHT_OUTER_MERGE;
    }

    @Test
    public void testExternalHashRightOuterJoinTask() throws Exception {

        final int keyCnt1 = 32768;
        final int valCnt1 = 8;

        final int keyCnt2 = 65536;
        final int valCnt2 = 8;

        final int expCnt = calculateExpectedCount(keyCnt1, valCnt1, keyCnt2, valCnt2);

        setOutput(this.output);
        addDriverComparator(this.comparator1);
        addDriverComparator(this.comparator2);
        getTaskConfig().setDriverPairComparator(new RuntimePairComparatorFactory());
        getTaskConfig().setDriverStrategy(DriverStrategy.RIGHT_HYBRIDHASH_BUILD_FIRST);
        getTaskConfig().setRelativeMemoryDriver(hash_frac);

        final AbstractOuterJoinDriver<
                        Tuple2<Integer, Integer>,
                        Tuple2<Integer, Integer>,
                        Tuple2<Integer, Integer>>
                testTask = getOuterJoinDriver();

        addInputSorted(
                new UniformIntTupleGenerator(keyCnt1, valCnt1, false),
                serializer,
                this.comparator1.duplicate());
        addInputSorted(
                new UniformIntTupleGenerator(keyCnt2, valCnt2, false),
                serializer,
                this.comparator2.duplicate());
        testDriver(testTask, MockJoinStub.class);

        Assert.assertEquals("Wrong result set size.", expCnt, this.output.getNumberOfRecords());
    }
}
