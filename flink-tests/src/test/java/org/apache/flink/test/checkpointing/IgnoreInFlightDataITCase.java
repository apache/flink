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
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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

    @Rule public final SharedObjects sharedObjects = SharedObjects.create();

    private static final int PARALLELISM = 3;

    private SharedReference<OneShotLatch> checkpointReachSinkLatch;
    private SharedReference<AtomicLong> resultBeforeFail;
    private SharedReference<AtomicLong> result;
    private SharedReference<AtomicInteger> lastCheckpointValue;
    private int checkpointInterval = 5;

    private static Configuration getConfiguration() {
        Configuration config = new Configuration();
        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("48m"));
        return config;
    }

    public void setupSharedObjects() {
        checkpointReachSinkLatch = sharedObjects.add(new OneShotLatch());
        resultBeforeFail = sharedObjects.add(new AtomicLong());
        result = sharedObjects.add(new AtomicLong());
        lastCheckpointValue = sharedObjects.add(new AtomicInteger());
    }

    /**
     * This test contains one Source, three Maps and one Sink. The two out of three Map are waiting
     * until signal from Sink(when the Sink receives the first checkpoint barrier). When Source
     * emits the data it is able to achieve Sink only via Map-0 because another Maps freeze. When
     * the checkpoint happens the barrier achieve the Sink also via Map-0 and at this moment there
     * is situation where some data were emitted from Source but didn't achieve the Sink yet. And
     * these are exact the data which should become the in-flight data because the Sink already
     * received the first checkpoint barrier. So, if Source sent at least one record to Map-1 or
     * Map-2 before checkpoint was triggered it should guarantee that the in-flight data in some
     * gate Source-Map or Map-Sink will exist. But if Source didn't send anything before the
     * checkpoint it will fail and it is exactly why this test contains the loop to do more
     * attempts.
     */
    @Test
    public void testIgnoreInFlightDataDuringRecovery() {
        while (!executeIgnoreInFlightDataDuringRecovery()) {
            // This test can fail if the first checkpoint happens before the Source emits some data.
            // In this case, the test will be restarted until it reach success or the test timeout
            // happens.
        }
    }

    private boolean executeIgnoreInFlightDataDuringRecovery() {
        // given: Stream which will fail after first checkpoint.
        setupSharedObjects();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        // Increase interval twice on each attempt in order to give more time for the Source to send
        // all required data before the first checkpoint.
        env.enableCheckpointing(checkpointInterval *= 2);
        env.disableOperatorChaining();
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        env.getCheckpointConfig().setAlignmentTimeout(Duration.ZERO);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointIdOfIgnoredInFlightData(1);
        env.setRestartStrategy(fixedDelayRestart(1, 0));

        env.addSource(new NumberSource(lastCheckpointValue))
                // map for having parallel execution.
                .map(new SlowMap(checkpointReachSinkLatch))
                .addSink(new SumFailSink(checkpointReachSinkLatch, resultBeforeFail, result))
                // one sink for easy calculation.
                .setParallelism(1);

        // when: Job is executed.
        try {
            env.execute("Total sum");
        } catch (Exception ex) {
            log.error("Execution failed", ex);
            return false;
        }

        // Calculate the expected single value after recovery.
        int sourceValueAfterRestore = lastCheckpointValue.get().intValue() + 1;

        // Calculate result in case of normal recovery.
        long resultWithoutIgnoringData = 0;
        for (int i = 0; i <= sourceValueAfterRestore; i++) {
            resultWithoutIgnoringData += i;
        }

        // then: Actual result should be less than the ideal result because some of data was
        // ignored.
        assertThat(result.get().longValue(), lessThan(resultWithoutIgnoringData));

        // and: Actual result should be equal to sum of result before fail + source value after
        // recovery.
        long expectedResult = resultBeforeFail.get().longValue() + sourceValueAfterRestore;
        assertEquals(expectedResult, result.get().longValue());

        return true;
    }

    private static class SumFailSink implements SinkFunction<Integer>, CheckpointedFunction {
        private final SharedReference<OneShotLatch> checkpointReachSinkLatch;
        private final SharedReference<AtomicLong> resultBeforeFail;
        private final SharedReference<AtomicLong> result;

        public SumFailSink(
                SharedReference<OneShotLatch> checkpointReachSinkLatch,
                SharedReference<AtomicLong> resultBeforeFail,
                SharedReference<AtomicLong> result) {
            this.checkpointReachSinkLatch = checkpointReachSinkLatch;
            this.resultBeforeFail = resultBeforeFail;
            this.result = result;
        }

        @Override
        public void invoke(Integer value, Context context) throws Exception {
            result.get().addAndGet(value);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // This job should fail on the checkpointId == 2 so remember the last successful
            // checkpoint before it.
            if (context.getCheckpointId() == 1) {
                resultBeforeFail.get().set(result.get().longValue());
                sinkCheckpointStarted();
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            result.get().set(resultBeforeFail.get().longValue());
        }

        /**
         * Allow to send data from the awaited map in this case if number of waiters more than 0, we
         * can be sure that in-flight data exists(at least the data which is processing by waiters
         * during the waiting will be sent to the sink before the checkpoint barrier would be
         * handled).
         */
        public void sinkCheckpointStarted() {
            checkpointReachSinkLatch.get().trigger();
        }
    }

    private static class NumberSource implements SourceFunction<Integer>, CheckpointedFunction {

        private static final long serialVersionUID = 1L;
        private final SharedReference<AtomicInteger> lastCheckpointValue;
        private ListState<Integer> valueState;
        private volatile boolean isRunning = true;

        public NumberSource(SharedReference<AtomicInteger> lastCheckpointValue) {
            this.lastCheckpointValue = lastCheckpointValue;
        }

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            Iterator<Integer> stateIt = valueState.get().iterator();
            boolean isRecovered = stateIt.hasNext();

            if (isRecovered) {
                synchronized (ctx.getCheckpointLock()) {
                    Integer lastValue = stateIt.next();

                    // Checking that ListState is recovered correctly.
                    assertEquals(lastCheckpointValue.get().intValue(), lastValue.intValue());

                    // if it is started after recovery, just send one more value and finish.
                    ctx.collect(lastValue + 1);
                }
            } else {
                int next = 0;

                synchronized (ctx.getCheckpointLock()) {
                    // Emit batch of data in order to having the downstream data for each subtask of
                    // the Map before the first checkpoint.
                    do {
                        next++;
                        valueState.update(singletonList(next));
                        ctx.collect(next);
                    } while (next < PARALLELISM); // One value for each map subtask is enough.
                }

                while (isRunning) {
                    // Wait for the checkpoint.
                    LockSupport.parkNanos(100000);
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            Iterator<Integer> integerIterator = valueState.get().iterator();

            if (!integerIterator.hasNext()
                    || integerIterator.next() < PARALLELISM
                    || (context.getCheckpointId() > 1
                            && lastCheckpointValue.get().get() < PARALLELISM)) {
                // Try to restart task.
                throw new RuntimeException(
                        "Not enough data to guarantee the in-flight data were generated before the first checkpoint");
            }

            if (context.getCheckpointId() > 2) {
                // It is possible if checkpoint was triggered too fast after restart.
                return; // Just ignore it.
            }

            if (context.getCheckpointId() == 2) {
                throw new ExpectedTestException("The planned fail on the second checkpoint");
            }

            lastCheckpointValue.get().set(valueState.get().iterator().next());
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            this.valueState =
                    context.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("state", Types.INT));
        }
    }

    private static class SlowMap extends RichMapFunction<Integer, Integer> {

        private final SharedReference<OneShotLatch> checkpointReachSinkLatch;

        public SlowMap(SharedReference<OneShotLatch> checkpointReachSinkLatch) {
            this.checkpointReachSinkLatch = checkpointReachSinkLatch;
        }

        @Override
        public Integer map(Integer value) throws Exception {
            // Allow working only one subtask until the checkpoint barrier reaches the sink.
            if (getRuntimeContext().getIndexOfThisSubtask() > 0) {
                checkpointReachSinkLatch.get().await();
            }

            return value;
        }
    }
}
