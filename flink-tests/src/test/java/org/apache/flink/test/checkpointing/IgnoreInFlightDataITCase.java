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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.Iterator;
import java.util.concurrent.locks.LockSupport;

import static java.util.Collections.singletonList;
import static org.apache.flink.api.common.restartstrategy.RestartStrategies.fixedDelayRestart;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;

/** Test of ignoring in-flight data during recovery. */
public class IgnoreInFlightDataITCase extends TestLogger {
    @ClassRule
    public static final MiniClusterWithClientResource CLUSTER =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(getConfiguration())
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    private static Configuration getConfiguration() {
        Configuration config = new Configuration();
        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("48m"));
        return config;
    }

    @Test
    public void testIgnoreInFlightDataDuringRecovery() throws Exception {
        // given: Stream which will fail after first checkpoint.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.enableCheckpointing(10);
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointIdOfIgnoredInFlightData(1);
        env.setRestartStrategy(fixedDelayRestart(2, 0));

        env.addSource(new NumberSource())
                .shuffle()
                // map for having parallel execution.
                .map(new SlowMap())
                .addSink(new SumFailSink())
                // one sink for easy calculation.
                .setParallelism(1);

        // when: Job is executed.
        env.execute("Total sum");

        // Calculate the expected single value after recovery.
        int sourceValueAfterRestore = NumberSource.lastCheckpointedValue + 1;

        // Calculate result in case of normal recovery.
        long resultWithoutIgnoringData = 0;
        for (int i = 0; i <= sourceValueAfterRestore; i++) {
            resultWithoutIgnoringData += i;
        }

        // then: Actual result should be less than the ideal result because some of data was
        // ignored.
        assertThat(SumFailSink.result, lessThan(resultWithoutIgnoringData));

        // and: Actual result should be equal to sum of result before fail + source value after
        // recovery.
        long expectedResult = SumFailSink.resultBeforeFail + sourceValueAfterRestore;
        assertEquals(expectedResult, SumFailSink.result);
    }

    private static class SumFailSink implements SinkFunction<Integer>, CheckpointedFunction {
        public static long result;
        public static long resultBeforeFail;

        @Override
        public void invoke(Integer value) throws Exception {
            result += value;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            resultBeforeFail = result;
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            result = resultBeforeFail;
        }
    }

    private static class NumberSource implements SourceFunction<Integer>, CheckpointedFunction {

        private static final long serialVersionUID = 1L;
        private ListState<Integer> valueState;
        public static int lastCheckpointedValue;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            Iterator<Integer> stateIt = valueState.get().iterator();
            boolean isRecovered = stateIt.hasNext();

            if (isRecovered) {
                Integer lastValue = stateIt.next();

                // Checking that ListState is recovered correctly.
                assertEquals(lastCheckpointedValue, lastValue.intValue());

                // if it is started after recovery, just send one more value and finish.
                ctx.collect(lastValue + 1);
            } else {
                int next = 0;
                while (true) {
                    synchronized (ctx.getCheckpointLock()) {
                        next++;
                        valueState.update(singletonList(next));
                        ctx.collect(next);
                    }
                }
            }
        }

        @Override
        public void cancel() {}

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            if (lastCheckpointedValue > 0) {
                throw new RuntimeException("Error during snapshot");
            }

            lastCheckpointedValue = valueState.get().iterator().next();
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            this.valueState =
                    context.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("state", Types.INT));
        }
    }

    private static class SlowMap extends RichMapFunction<Integer, Integer> {

        @Override
        public Integer map(Integer value) throws Exception {
            // slow down the map in order to have more intermediate data.
            LockSupport.parkNanos(100000);
            return value;
        }
    }
}
