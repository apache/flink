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

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.state.rocksdb.RocksDBOptions;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.legacy.RichParallelSourceFunction;
import org.apache.flink.streaming.util.CheckpointStorageUtils;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.streaming.util.StateBackendUtils;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * A simple test that runs a streaming topology with checkpointing enabled.
 *
 * <p>The test triggers a failure after a while and verifies that, after completion, the state
 * reflects the "exactly once" semantics.
 *
 * <p>It is designed to check partitioned states.
 */
@SuppressWarnings("serial")
public class KeyedStateCheckpointingITCase extends TestLogger {

    protected static final int MAX_MEM_STATE_SIZE = 10 * 1024 * 1024;

    protected static final int NUM_STRINGS = 10_000;
    protected static final int NUM_KEYS = 40;

    protected static final int NUM_TASK_MANAGERS = 2;
    protected static final int NUM_TASK_SLOTS = 2;
    protected static final int PARALLELISM = NUM_TASK_MANAGERS * NUM_TASK_SLOTS;

    // ------------------------------------------------------------------------

    @ClassRule
    public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(getConfiguration())
                            .setNumberTaskManagers(NUM_TASK_MANAGERS)
                            .setNumberSlotsPerTaskManager(NUM_TASK_SLOTS)
                            .build());

    private static Configuration getConfiguration() {
        Configuration config = new Configuration();
        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("12m"));
        return config;
    }

    // ------------------------------------------------------------------------

    @Rule public final TemporaryFolder tmpFolder = new TemporaryFolder();

    @Test
    public void testWithMemoryBackendAsync() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StateBackendUtils.configureHashMapStateBackend(env);
        CheckpointStorageUtils.configureJobManagerCheckpointStorage(env);
        testProgramWithBackend(env);
    }

    @Test
    public void testWithFsBackendAsync() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StateBackendUtils.configureHashMapStateBackend(env);
        CheckpointStorageUtils.configureFileSystemCheckpointStorage(
                env, tmpFolder.newFolder().toURI().toString());
        testProgramWithBackend(env);
    }

    @Test
    public void testWithRocksDbBackendFull() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StateBackendUtils.configureRocksDBStateBackend(env, false);
        env.configure(
                new Configuration()
                        .set(
                                RocksDBOptions.LOCAL_DIRECTORIES,
                                tmpFolder.newFolder().getAbsolutePath()));
        CheckpointStorageUtils.configureFileSystemCheckpointStorage(
                env, tmpFolder.newFolder().toURI().toString());
        testProgramWithBackend(env);
    }

    @Test
    public void testWithRocksDbBackendIncremental() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StateBackendUtils.configureRocksDBStateBackend(env, true);
        env.configure(
                new Configuration()
                        .set(
                                RocksDBOptions.LOCAL_DIRECTORIES,
                                tmpFolder.newFolder().getAbsolutePath()));
        CheckpointStorageUtils.configureFileSystemCheckpointStorage(
                env, tmpFolder.newFolder().toURI().toString());
        testProgramWithBackend(env);
    }

    // ------------------------------------------------------------------------

    protected void testProgramWithBackend(StreamExecutionEnvironment env) throws Exception {
        assertEquals("Broken test setup", 0, (NUM_STRINGS / 2) % NUM_KEYS);

        env.setParallelism(PARALLELISM);
        env.enableCheckpointing(500);
        RestartStrategyUtils.configureFixedDelayRestartStrategy(env, Integer.MAX_VALUE, 0L);

        // compute when (randomly) the failure should happen
        final int failurePosMin = (int) (0.6 * NUM_STRINGS / PARALLELISM);
        final int failurePosMax = (int) (0.8 * NUM_STRINGS / PARALLELISM);
        final int failurePos =
                (new Random().nextInt(failurePosMax - failurePosMin) + failurePosMin);

        final DataStream<Integer> stream1 =
                env.addSource(new IntGeneratingSourceFunction(NUM_STRINGS / 2, NUM_STRINGS / 4));

        final DataStream<Integer> stream2 =
                env.addSource(new IntGeneratingSourceFunction(NUM_STRINGS / 2, NUM_STRINGS / 4));

        stream1.union(stream2)
                .keyBy(new IdentityKeySelector<Integer>())
                .map(new OnceFailingPartitionedSum(failurePos))
                .keyBy(x -> x.f0)
                .addSink(new CounterSink());

        env.execute();

        // verify that we counted exactly right
        assertEquals(NUM_KEYS, CounterSink.ALL_COUNTS.size());
        assertEquals(NUM_KEYS, OnceFailingPartitionedSum.ALL_SUMS.size());

        for (Entry<Integer, Long> sum : OnceFailingPartitionedSum.ALL_SUMS.entrySet()) {
            assertEquals((long) sum.getKey() * NUM_STRINGS / NUM_KEYS, sum.getValue().longValue());
        }
        for (long count : CounterSink.ALL_COUNTS.values()) {
            assertEquals(NUM_STRINGS / NUM_KEYS, count);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Custom Functions
    // --------------------------------------------------------------------------------------------

    /**
     * A source that generates a sequence of integers and throttles down until a checkpoint has
     * happened.
     */
    private static class IntGeneratingSourceFunction extends RichParallelSourceFunction<Integer>
            implements ListCheckpointed<Integer>, CheckpointListener {

        private final int numElements;
        private final int checkpointLatestAt;

        private int lastEmitted = -1;

        private boolean checkpointHappened;

        private volatile boolean isRunning = true;

        IntGeneratingSourceFunction(int numElements, int checkpointLatestAt) {
            this.numElements = numElements;
            this.checkpointLatestAt = checkpointLatestAt;
        }

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            final Object lockingObject = ctx.getCheckpointLock();
            final int step = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();

            int nextElement =
                    lastEmitted >= 0
                            ? lastEmitted + step
                            : getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();

            while (isRunning && nextElement < numElements) {

                // throttle / block if we are still waiting for the checkpoint
                if (!checkpointHappened) {
                    if (nextElement < checkpointLatestAt) {
                        // only throttle
                        Thread.sleep(1);
                    } else {
                        // hard block
                        synchronized (this) {
                            while (!checkpointHappened) {
                                this.wait();
                            }
                        }
                    }
                }

                //noinspection SynchronizationOnLocalVariableOrMethodParameter
                synchronized (lockingObject) {
                    ctx.collect(nextElement % NUM_KEYS);
                    lastEmitted = nextElement;
                }

                nextElement += step;
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(lastEmitted);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            assertEquals("Test failed due to unexpected recovered state size", 1, state.size());
            lastEmitted = state.get(0);
            checkpointHappened = true;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            synchronized (this) {
                checkpointHappened = true;
                this.notifyAll();
            }
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) {}
    }

    private static class OnceFailingPartitionedSum
            extends RichMapFunction<Integer, Tuple2<Integer, Long>>
            implements ListCheckpointed<Integer> {

        private static final Map<Integer, Long> ALL_SUMS = new ConcurrentHashMap<>();

        private final int failurePos;
        private int count;

        private boolean shouldFail = true;

        private transient ValueState<Long> sum;

        OnceFailingPartitionedSum(int failurePos) {
            this.failurePos = failurePos;
        }

        @Override
        public void open(OpenContext openContext) throws IOException {
            sum = getRuntimeContext().getState(new ValueStateDescriptor<>("my_state", Long.class));
        }

        @Override
        public Tuple2<Integer, Long> map(Integer value) throws Exception {
            if (shouldFail && count++ >= failurePos) {
                shouldFail = false;
                throw new Exception("Test Failure");
            }

            Long oldSum = sum.value();
            long currentSum = (oldSum == null ? 0L : oldSum) + value;

            sum.update(currentSum);
            ALL_SUMS.put(value, currentSum);
            return new Tuple2<>(value, currentSum);
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(count);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            assertEquals("Test failed due to unexpected recovered state size", 1, state.size());
            count = state.get(0);
            shouldFail = false;
        }

        @Override
        public void close() throws Exception {
            if (shouldFail) {
                fail("Test ineffective: Function cleanly finished without ever failing.");
            }
        }
    }

    private static class CounterSink extends RichSinkFunction<Tuple2<Integer, Long>> {

        private static final Map<Integer, Long> ALL_COUNTS = new ConcurrentHashMap<>();

        private transient ValueState<NonSerializableLong> aCounts;
        private transient ValueState<Long> bCounts;

        @Override
        public void open(OpenContext openContext) throws IOException {
            aCounts =
                    getRuntimeContext()
                            .getState(new ValueStateDescriptor<>("a", NonSerializableLong.class));
            bCounts = getRuntimeContext().getState(new ValueStateDescriptor<>("b", Long.class));
        }

        @Override
        public void invoke(Tuple2<Integer, Long> value) throws Exception {
            final NonSerializableLong acRaw = aCounts.value();
            final Long bcRaw = bCounts.value();

            final long ac = acRaw == null ? 0L : acRaw.value;
            final long bc = bcRaw == null ? 0L : bcRaw;

            assertEquals(ac, bc);

            long currentCount = ac + 1;
            aCounts.update(NonSerializableLong.of(currentCount));
            bCounts.update(currentCount);

            ALL_COUNTS.put(value.f0, currentCount);
        }
    }

    private static class IdentityKeySelector<T> implements KeySelector<T, T> {

        @Override
        public T getKey(T value) throws Exception {
            return value;
        }
    }

    // ------------------------------------------------------------------------
    //  data types
    // ------------------------------------------------------------------------

    /** Custom boxed long type that does not implement Serializable. */
    public static class NonSerializableLong {

        public long value;

        private NonSerializableLong(long value) {
            this.value = value;
        }

        public static NonSerializableLong of(long value) {
            return new NonSerializableLong(value);
        }

        @Override
        public boolean equals(Object obj) {
            return this == obj
                    || obj != null
                            && obj.getClass() == getClass()
                            && ((NonSerializableLong) obj).value == this.value;
        }

        @Override
        public int hashCode() {
            return (int) (value ^ (value >>> 32));
        }
    }
}
