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
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.BatchTask;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.FlatMapDriver;
import org.apache.flink.runtime.operators.FlatMapTaskTest.MockMapStub;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.testutils.TaskTestBase;
import org.apache.flink.runtime.operators.testutils.UniformRecordGenerator;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.testutils.recordutils.RecordComparatorFactory;
import org.apache.flink.runtime.testutils.recordutils.RecordSerializerFactory;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ChainedAllReduceDriverTest extends TaskTestBase {

    private static final int MEMORY_MANAGER_SIZE = 1024 * 1024 * 3;

    private static final int NETWORK_BUFFER_SIZE = 1024;

    private final List<Record> outList = new ArrayList<>();

    @SuppressWarnings("unchecked")
    private final RecordComparatorFactory compFact =
            new RecordComparatorFactory(
                    new int[] {0}, new Class[] {IntValue.class}, new boolean[] {true});

    private final RecordSerializerFactory serFact = RecordSerializerFactory.get();

    @Test
    public void testMapTask() throws Exception {
        final int keyCnt = 100;
        final int valCnt = 20;

        final double memoryFraction = 1.0;

        // environment
        initEnvironment(MEMORY_MANAGER_SIZE, NETWORK_BUFFER_SIZE);
        mockEnv.getExecutionConfig().enableObjectReuse();
        addInput(new UniformRecordGenerator(keyCnt, valCnt, false), 0);
        addOutput(this.outList);

        // chained reduce config
        {
            final TaskConfig reduceConfig = new TaskConfig(new Configuration());

            // input
            reduceConfig.addInputToGroup(0);
            reduceConfig.setInputSerializer(serFact, 0);

            // output
            reduceConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
            reduceConfig.setOutputSerializer(serFact);

            // driver
            reduceConfig.setDriverStrategy(DriverStrategy.ALL_REDUCE);
            reduceConfig.setDriverComparator(compFact, 0);
            reduceConfig.setDriverComparator(compFact, 1);
            reduceConfig.setRelativeMemoryDriver(memoryFraction);

            // udf
            reduceConfig.setStubWrapper(new UserCodeClassWrapper<>(MockReduceStub.class));

            getTaskConfig().addChainedTask(ChainedAllReduceDriver.class, reduceConfig, "reduce");
        }

        // chained map+reduce
        {
            registerTask(FlatMapDriver.class, MockMapStub.class);
            BatchTask<FlatMapFunction<Record, Record>, Record> testTask = new BatchTask<>(mockEnv);

            testTask.invoke();
        }

        int sumTotal = valCnt * keyCnt * (keyCnt - 1) / 2;

        Assert.assertEquals(1, this.outList.size());
        Assert.assertEquals(sumTotal, this.outList.get(0).getField(0, IntValue.class).getValue());
    }

    public static class MockReduceStub implements ReduceFunction<Record> {
        private static final long serialVersionUID = 1047525105526690165L;

        @Override
        public Record reduce(Record value1, Record value2) throws Exception {
            IntValue v1 = value1.getField(0, IntValue.class);
            IntValue v2 = value2.getField(0, IntValue.class);

            // set value and force update of record; this updates and returns
            // value1 in order to test ChainedAllReduceDriver.collect() when
            // object reuse is enabled
            v1.setValue(v1.getValue() + v2.getValue());
            value1.setField(0, v1);
            value1.updateBinaryRepresenation();
            return value1;
        }
    }
}
