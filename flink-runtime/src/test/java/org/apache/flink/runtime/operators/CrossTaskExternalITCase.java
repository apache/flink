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
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.runtime.operators.CrossTaskTest.MockCrossStub;
import org.apache.flink.runtime.operators.testutils.DriverTestBase;
import org.apache.flink.runtime.operators.testutils.UniformRecordGenerator;
import org.apache.flink.types.Record;

import org.junit.Assert;
import org.junit.Test;

public class CrossTaskExternalITCase extends DriverTestBase<CrossFunction<Record, Record, Record>> {
    private static final long CROSS_MEM = 1024 * 1024;

    private final double cross_frac;

    private final CountingOutputCollector output = new CountingOutputCollector();

    public CrossTaskExternalITCase(ExecutionConfig config) {
        super(config, CROSS_MEM, 0);
        cross_frac = (double) CROSS_MEM / this.getMemoryManager().getMemorySize();
    }

    @Test
    public void testExternalBlockCrossTask() {

        int keyCnt1 = 2;
        int valCnt1 = 1;

        // 43690 fit into memory, 43691 do not!
        int keyCnt2 = 43700;
        int valCnt2 = 1;

        final int expCnt = keyCnt1 * valCnt1 * keyCnt2 * valCnt2;

        setOutput(this.output);

        addInput(new UniformRecordGenerator(keyCnt1, valCnt1, false));
        addInput(new UniformRecordGenerator(keyCnt2, valCnt2, false));

        getTaskConfig().setDriverStrategy(DriverStrategy.NESTEDLOOP_BLOCKED_OUTER_FIRST);
        getTaskConfig().setRelativeMemoryDriver(cross_frac);

        final CrossDriver<Record, Record, Record> testTask =
                new CrossDriver<Record, Record, Record>();

        try {
            testDriver(testTask, MockCrossStub.class);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Test failed due to an exception.");
        }

        Assert.assertEquals("Wrong result size.", expCnt, this.output.getNumberOfRecords());
    }

    @Test
    public void testExternalStreamCrossTask() {

        int keyCnt1 = 2;
        int valCnt1 = 1;

        // 87381 fit into memory, 87382 do not!
        int keyCnt2 = 87385;
        int valCnt2 = 1;

        final int expCnt = keyCnt1 * valCnt1 * keyCnt2 * valCnt2;

        setOutput(this.output);

        addInput(new UniformRecordGenerator(keyCnt1, valCnt1, false));
        addInput(new UniformRecordGenerator(keyCnt2, valCnt2, false));

        getTaskConfig().setDriverStrategy(DriverStrategy.NESTEDLOOP_STREAMED_OUTER_FIRST);
        getTaskConfig().setRelativeMemoryDriver(cross_frac);

        final CrossDriver<Record, Record, Record> testTask =
                new CrossDriver<Record, Record, Record>();

        try {
            testDriver(testTask, MockCrossStub.class);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Test failed due to an exception.");
        }

        Assert.assertEquals("Wrong result size.", expCnt, this.output.getNumberOfRecords());
    }
}
