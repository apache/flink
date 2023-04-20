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
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.TestLogger;

import org.jetbrains.annotations.NotNull;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.apache.flink.test.checkpointing.RestoreUpgradedJobITCase.MapName.MAP_1;
import static org.apache.flink.test.checkpointing.RestoreUpgradedJobITCase.MapName.MAP_2;
import static org.apache.flink.test.checkpointing.RestoreUpgradedJobITCase.MapName.MAP_3;
import static org.apache.flink.test.checkpointing.RestoreUpgradedJobITCase.MapName.MAP_4;
import static org.apache.flink.test.checkpointing.RestoreUpgradedJobITCase.MapName.MAP_5;
import static org.apache.flink.test.checkpointing.RestoreUpgradedJobITCase.MapName.MAP_6;
import static org.apache.flink.test.checkpointing.RestoreUpgradedJobITCase.TestCheckpointType.ALIGNED_CHECKPOINT;
import static org.apache.flink.test.checkpointing.RestoreUpgradedJobITCase.TestCheckpointType.CANONICAL_SAVEPOINT;
import static org.apache.flink.test.checkpointing.RestoreUpgradedJobITCase.TestCheckpointType.NATIVE_SAVEPOINT;
import static org.apache.flink.util.Preconditions.checkState;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Test check scenario when the upgraded job(different map order, different record type, new map)
 * restored on old savepoint/checkpoint.
 */
@RunWith(Parameterized.class)
public class RestoreUpgradedJobITCase extends TestLogger {
    private static final int PARALLELISM = 4;
    private static final int TOTAL_RECORDS = 100;

    @ClassRule public static TemporaryFolder temporaryFolder = new TemporaryFolder();
    @Parameterized.Parameter public TestCheckpointType checkpointType;

    @ClassRule
    public static final MiniClusterWithClientResource CLUSTER =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(new Configuration())
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(4)
                            .build());

    @Rule public final SharedObjects sharedObjects = SharedObjects.create();

    private SharedReference<OneShotLatch> allDataEmittedLatch;
    private SharedReference<AtomicLong> result;

    public void setupSharedObjects() {
        allDataEmittedLatch = sharedObjects.add(new OneShotLatch());
        result = sharedObjects.add(new AtomicLong());
    }

    @Parameterized.Parameters(name = "Savepoint type[{0}]")
    public static Object[][] parameters() {
        return new Object[][] {
            {ALIGNED_CHECKPOINT}, {CANONICAL_SAVEPOINT}, {NATIVE_SAVEPOINT},
        };
    }

    enum TestCheckpointType {
        ALIGNED_CHECKPOINT,
        CANONICAL_SAVEPOINT,
        NATIVE_SAVEPOINT
    }

    enum MapName {
        MAP_1,
        MAP_2,
        MAP_3,
        MAP_4,
        MAP_5,
        MAP_6;

        int id() {
            return ordinal() + 1;
        }
    }

    @Test
    public void testRestoreUpgradedJob() throws Exception {
        setupSharedObjects();

        // when: Run original job.
        String snapshotPath = runOriginalJob();

        // then: Check the result before the checkpoint.
        assertThat(result.get().longValue(), is(calculateExpectedResultBeforeSavepoint()));
        result.get().set(0);

        // when: Executing the new job with different order of maps.
        runUpgradedJob(snapshotPath);

        // then: The final result should ignore state from new maps(because it is empty).
        assertThat(result.get().longValue(), is(calculateExpectedResultBeforeSavepoint()));
    }

    private long calculateExpectedResultAfterSavepoint() {
        long totalStates = 0;
        for (int i = 1; i <= MapName.values().length; i++) {
            totalStates += (long) i * i;
        }
        long expectedAfterSavepointResult = 0;
        for (int i = 0; i < TOTAL_RECORDS; i++) {
            expectedAfterSavepointResult += i + totalStates;
        }
        // Multiply for parallelism due to broadcast.
        return PARALLELISM * expectedAfterSavepointResult;
    }

    private long calculateExpectedResultBeforeSavepoint() {
        long expectedBeforeSavepointResult = 0;
        for (int i = 0; i < TOTAL_RECORDS; i++) {
            expectedBeforeSavepointResult += i;
        }
        // Multiply for parallelism due to broadcast.
        return PARALLELISM * expectedBeforeSavepointResult;
    }

    @NotNull
    private String runOriginalJob() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig()
                .setExternalizedCheckpointCleanup(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().enableUnalignedCheckpoints(false);
        env.getCheckpointConfig()
                .setCheckpointStorage("file://" + temporaryFolder.getRoot().getAbsolutePath());
        env.setParallelism(PARALLELISM);
        env.enableCheckpointing(Integer.MAX_VALUE);

        // Different order of maps before and after savepoint.
        env.addSource(new IntSource(allDataEmittedLatch))
                .map(new IntMap(MAP_5.id()))
                .uid(MAP_5.name())
                .forward()
                .map(new IntMap(MAP_1.id()))
                .uid(MAP_1.name())
                .slotSharingGroup("anotherSharingGroup")
                .keyBy((key) -> key)
                .map(new IntMap(MAP_6.id()))
                .uid(MAP_6.name())
                .rebalance()
                .map(new IntMap(MAP_4.id()))
                .uid(MAP_4.name())
                .broadcast()
                .map(new IntMap(MAP_2.id()))
                .uid(MAP_2.name())
                .rescale()
                .map(new IntMap(MAP_3.id()))
                .uid(MAP_3.name())
                .addSink(new IntSink(result))
                // one sink for easy calculation.
                .setParallelism(1);

        // when: Job is executed.
        JobClient jobClient = env.executeAsync("Total sum");
        waitForAllTaskRunning(CLUSTER.getMiniCluster(), jobClient.getJobID(), false);

        allDataEmittedLatch.get().await();
        allDataEmittedLatch.get().reset();

        return stopWithSnapshot(jobClient);
    }

    private void runUpgradedJob(String snapshotPath) throws Exception {
        StreamExecutionEnvironment env;
        Configuration conf = new Configuration();
        conf.set(SavepointConfigOptions.SAVEPOINT_PATH, snapshotPath);
        env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(PARALLELISM);
        env.addSource(new StringSource(allDataEmittedLatch))
                .map(new StringMap(MAP_1.id()))
                .uid(MAP_1.name())
                .forward()
                .map(new StringMap(MAP_2.id()))
                .uid(MAP_2.name())
                .slotSharingGroup("anotherSharingGroup")
                .keyBy((key) -> key)
                .map(new StringMap(MAP_3.id()))
                .uid(MAP_3.name())
                .map(new StringMap(-1))
                .uid("new_chained_map")
                .rebalance()
                .map(new StringMap(-2))
                .uid("new_map2")
                .map(new StringMap(MAP_4.id()))
                .uid(MAP_4.name())
                .rescale()
                .map(new StringMap(MAP_5.id()))
                .uid(MAP_5.name())
                .broadcast()
                .map(new StringMap(MAP_6.id()))
                .uid(MAP_6.name())
                .addSink(new StringSink(result))
                // one sink for easy calculation.
                .setParallelism(1);

        JobClient jobClient = env.executeAsync("Total sum");

        waitForAllTaskRunning(CLUSTER.getMiniCluster(), jobClient.getJobID(), false);

        allDataEmittedLatch.get().await();

        // Using stopWithSavepoint to be sure that all values reached the sink.
        jobClient
                .stopWithSavepoint(
                        true,
                        temporaryFolder.getRoot().getAbsolutePath(),
                        SavepointFormatType.CANONICAL)
                .get();
    }

    private String stopWithSnapshot(JobClient jobClient)
            throws InterruptedException, ExecutionException {
        String snapshotPath;
        if (checkpointType == ALIGNED_CHECKPOINT) {
            snapshotPath = CLUSTER.getMiniCluster().triggerCheckpoint(jobClient.getJobID()).get();
            jobClient.cancel().get();
        } else if (checkpointType == CANONICAL_SAVEPOINT) {
            snapshotPath =
                    jobClient
                            .stopWithSavepoint(
                                    true,
                                    temporaryFolder.getRoot().getAbsolutePath(),
                                    SavepointFormatType.CANONICAL)
                            .get();
        } else if (checkpointType == NATIVE_SAVEPOINT) {
            snapshotPath =
                    jobClient
                            .stopWithSavepoint(
                                    true,
                                    temporaryFolder.getRoot().getAbsolutePath(),
                                    SavepointFormatType.NATIVE)
                            .get();
        } else {
            throw new IllegalArgumentException("Unknown checkpoint type: " + checkpointType);
        }
        return snapshotPath;
    }

    private static class IntSink implements SinkFunction<Integer> {
        private final SharedReference<AtomicLong> result;

        public IntSink(SharedReference<AtomicLong> result) {
            this.result = result;
        }

        @Override
        public void invoke(Integer value, Context context) throws Exception {
            result.get().addAndGet(value);
        }
    }

    private static class StringSink implements SinkFunction<String> {
        private final SharedReference<AtomicLong> result;

        public StringSink(SharedReference<AtomicLong> result) {
            this.result = result;
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            result.get().addAndGet(Integer.parseInt(value));
        }
    }

    private static class IntMap extends AbstractMap<Integer> {

        private IntMap(int id) {
            super(id);
        }

        @Override
        public Integer map(Integer value) throws Exception {
            return calculate(value);
        }
    }

    private static class StringMap extends AbstractMap<String> {

        private StringMap(int id) {
            super(id);
        }

        @Override
        public String map(String value) throws Exception {
            return String.valueOf(calculate(Integer.parseInt(value)));
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            super.initializeState(context);
            Iterator<Integer> iterator = valueState.get().iterator();

            // id less than 0 represents operators which weren't presented in snapshot.
            if (id > 0) {
                checkState(iterator.hasNext(), "Value state can not be empty.");
                Integer state = iterator.next();
                checkState(
                        id == state,
                        String.format("Value state(%s) should be equal to id(%s).", state, id));
            }

            checkState(!iterator.hasNext(), "Value state should be empty.");
        }
    }

    private abstract static class AbstractMap<T> extends RichMapFunction<T, T>
            implements CheckpointedFunction {
        protected ListState<Integer> valueState;
        protected final int id;

        private AbstractMap(int id) {
            this.id = id;
        }

        protected int calculate(int value) throws Exception {
            return value;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            valueState.add(id);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            this.valueState =
                    context.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("state", Types.INT));
        }
    }

    private static class IntSource extends TestSource<Integer> {
        public IntSource(SharedReference<OneShotLatch> dataEmitted) {
            super(dataEmitted);
        }

        @Override
        void collect(SourceContext<Integer> ctx, int index) {
            ctx.collect(index);
        }
    }

    private static class StringSource extends TestSource<String> {
        public StringSource(SharedReference<OneShotLatch> dataEmitted) {
            super(dataEmitted);
        }

        @Override
        void collect(SourceContext<String> ctx, int index) {
            ctx.collect(String.valueOf(index));
        }
    }

    private abstract static class TestSource<T> implements SourceFunction<T> {

        private static final long serialVersionUID = 1L;
        private final SharedReference<OneShotLatch> dataEmitted;

        private volatile boolean isRunning = true;

        public TestSource(SharedReference<OneShotLatch> dataEmitted) {
            this.dataEmitted = dataEmitted;
        }

        @Override
        public void run(SourceContext<T> ctx) throws Exception {
            int i = TOTAL_RECORDS;
            while (i-- > 0) {
                synchronized (ctx.getCheckpointLock()) {
                    collect(ctx, i);
                }
            }
            dataEmitted.get().trigger();
            while (isRunning) {
                LockSupport.parkNanos(100000);
            }
        }

        abstract void collect(SourceContext<T> ctx, int index);

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
