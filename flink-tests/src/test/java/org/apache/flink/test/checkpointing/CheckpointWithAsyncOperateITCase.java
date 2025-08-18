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
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.changelog.fs.FsStateChangelogStorageFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.RetriableAsyncOperateException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.time.Duration;
import java.util.Iterator;

import static org.apache.flink.test.util.TestUtils.submitJobAndWaitForResult;

/** Test checkpointing with async operate. */
public class CheckpointWithAsyncOperateITCase extends TestLogger {

    protected static final int NUM_TASK_MANAGERS = 3;
    protected static final int NUM_TASK_SLOTS = 4;
    protected static final int PARALLELISM = NUM_TASK_MANAGERS * NUM_TASK_SLOTS;
    protected static final int CHECKPOINT_LIMIT = 5;

    private static MiniClusterWithClientResource cluster;

    @ClassRule public static TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setup() throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY.key(), "full");

        // Configure DFS DSTL for this test as it might produce too much GC pressure if
        // ChangelogStateBackend is used.
        // Doing it on cluster level unconditionally as randomization currently happens on the job
        // level (environment); while this factory can only be set on the cluster level.
        FsStateChangelogStorageFactory.configure(
                configuration, tempFolder.newFolder(), Duration.ofMinutes(1), 10);
        cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(configuration)
                                .setNumberTaskManagers(NUM_TASK_MANAGERS)
                                .setNumberSlotsPerTaskManager(NUM_TASK_SLOTS)
                                .build());
        cluster.before();
    }

    @After
    public void shutDownExistingCluster() {
        if (cluster != null) {
            cluster.after();
            cluster = null;
        }
    }

    @Test
    public void testAsyncOperate() throws Exception {
        try {
            StreamExecutionEnvironment env = getEnvironment();
            env.addSource(new CustomSource())
                    .map(new StatefulMapFunction(CHECKPOINT_LIMIT))
                    .sinkTo(new DiscardingSink<Tuple2<Integer, Long>>());

            JobGraph jobGraph = env.getStreamGraph().getJobGraph();
            try {
                submitJobAndWaitForResult(
                        cluster.getClusterClient(), jobGraph, getClass().getClassLoader());
            } catch (Exception e) {
                Assert.assertTrue(
                        ExceptionUtils.findThrowable(e, SuccessException.class).isPresent());
            }
            for (int i = 0; i < PARALLELISM; i++) {
                Assert.assertEquals(
                        "The number of async snapshot state should be equal to the number of "
                                + "checkpoints.",
                        CHECKPOINT_LIMIT,
                        StatefulMapFunction.asyncOperateCounter[i]);
            }

            clearCounters();

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testAsyncOperateFailed() throws Exception {
        try {
            StreamExecutionEnvironment env = getEnvironment();
            env.addSource(new CustomSource())
                    .map(new StatefulMapFunctionWithAsyncOperateException(CHECKPOINT_LIMIT))
                    .sinkTo(new DiscardingSink<Tuple2<Integer, Long>>());

            JobGraph jobGraph = env.getStreamGraph().getJobGraph();
            try {
                submitJobAndWaitForResult(
                        cluster.getClusterClient(), jobGraph, getClass().getClassLoader());
            } catch (Exception e) {
                Assert.assertTrue(
                        ExceptionUtils.findThrowable(e, RuntimeException.class).isPresent());
            }
            // If asyncOperate fails, the task fails by default and subsequent checkpoint is
            // not triggered
            for (int i = 0; i < PARALLELISM; i++) {
                Assert.assertEquals(
                        "The number of async snapshot state should be equal to the number of "
                                + "checkpoints.",
                        CHECKPOINT_LIMIT / 2,
                        StatefulMapFunctionWithAsyncOperateException.asyncOperateCounter[i]);
            }
            clearCounters();

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testAsyncOperateFailedWithRetriableException() throws Exception {
        try {
            StreamExecutionEnvironment env = getEnvironment();
            env.addSource(new CustomSource())
                    .map(
                            new StatefulMapFunctionWithAsyncOperateRetriableException(
                                    CHECKPOINT_LIMIT))
                    .sinkTo(new DiscardingSink<Tuple2<Integer, Long>>());

            JobGraph jobGraph = env.getStreamGraph().getJobGraph();
            try {
                submitJobAndWaitForResult(
                        cluster.getClusterClient(), jobGraph, getClass().getClassLoader());
            } catch (Exception e) {
                Assert.assertTrue(
                        ExceptionUtils.findThrowable(e, SuccessException.class).isPresent());
            }
            // If asyncOperate fails with retriable exception, the current checkpoint fails,
            // but the task will not fail and subsequent checkpoint is triggered
            for (int i = 0; i < PARALLELISM; i++) {
                Assert.assertEquals(
                        "The number of async snapshot state should be equal to the number of "
                                + "checkpoints.",
                        CHECKPOINT_LIMIT - 1,
                        StatefulMapFunctionWithAsyncOperateRetriableException.asyncOperateCounter[
                                i]);
            }
            clearCounters();

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    private StreamExecutionEnvironment getEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.enableCheckpointing(500);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);
        RestartStrategyUtils.configureNoRestartStrategy(env);
        return env;
    }

    private void clearCounters() {
        for (int i = 0; i < PARALLELISM; i++) {
            StatefulMapFunction.asyncOperateCounter[i] = 0;
            StatefulMapFunctionWithAsyncOperateException.asyncOperateCounter[i] = 0;
            StatefulMapFunctionWithAsyncOperateRetriableException.asyncOperateCounter[i] = 0;
        }
    }

    private static class CustomSource implements SourceFunction<Tuple2<Integer, Long>> {

        private boolean running = true;

        private long count;

        @Override
        public void run(SourceFunction.SourceContext<Tuple2<Integer, Long>> ctx) throws Exception {

            Object lock = ctx.getCheckpointLock();

            while (running) {
                synchronized (lock) {
                    for (int i = 0; i < PARALLELISM; i++) {
                        ctx.collect(Tuple2.of(i, count + 1));
                    }
                    count++;
                }
                Thread.sleep(100);
            }
        }

        @Override
        public void cancel() {
            this.running = false;
        }
    }

    private static class StatefulMapFunction
            extends RichMapFunction<Tuple2<Integer, Long>, Tuple2<Integer, Long>>
            implements CheckpointedFunction, CheckpointListener {

        private long checkpointID;

        private ListState<Long> listState;

        private long completedCheckpointLimit;

        private long notifyCheckpointCounter;

        private static int[] asyncOperateCounter = new int[NUM_TASK_MANAGERS * NUM_TASK_SLOTS];

        public StatefulMapFunction(long completedCheckpointLimit) {
            this.completedCheckpointLimit = completedCheckpointLimit;
        }

        @Override
        public Tuple2<Integer, Long> map(Tuple2<Integer, Long> value) throws Exception {
            return new Tuple2<>(value.f0, value.f1 + checkpointID);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            if (context.getCheckpointId() > completedCheckpointLimit) {
                throw new SuccessException();
            }
            listState.add(context.getCheckpointId());
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            listState =
                    context.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("checkpoint-01", Types.LONG));

            Iterator<Long> iterator = listState.get().iterator();
            if (iterator.hasNext()) {
                this.checkpointID = iterator.next();
            }
        }

        @Override
        public void asyncOperate(FunctionSnapshotContext context) throws Exception {
            Thread.sleep(100);
            int index = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            asyncOperateCounter[index] = asyncOperateCounter[index] + 1;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            notifyCheckpointCounter++;
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }

    private static class StatefulMapFunctionWithAsyncOperateException
            extends RichMapFunction<Tuple2<Integer, Long>, Tuple2<Integer, Long>>
            implements CheckpointedFunction, CheckpointListener {

        private long checkpointID;

        private ListState<Long> listState;

        private long completedCheckpointLimit;

        private long notifyCheckpointCounter;

        private static int[] asyncOperateCounter = new int[NUM_TASK_MANAGERS * NUM_TASK_SLOTS];

        public StatefulMapFunctionWithAsyncOperateException(long completedCheckpointLimit) {
            this.completedCheckpointLimit = completedCheckpointLimit;
        }

        @Override
        public Tuple2<Integer, Long> map(Tuple2<Integer, Long> value) throws Exception {
            return new Tuple2<>(value.f0, value.f1 + checkpointID);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            if (context.getCheckpointId() > completedCheckpointLimit) {
                throw new SuccessException();
            }
            listState.add(context.getCheckpointId());
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            listState =
                    context.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("checkpoint-01", Types.LONG));

            Iterator<Long> iterator = listState.get().iterator();
            if (iterator.hasNext()) {
                this.checkpointID = iterator.next();
            }
        }

        @Override
        public void asyncOperate(FunctionSnapshotContext context) throws Exception {
            if (context.getCheckpointId() == (completedCheckpointLimit / 2 + 1)) {
                throw new RuntimeException("asyncOperate error");
            }
            Thread.sleep(100);
            int index = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            asyncOperateCounter[index] = asyncOperateCounter[index] + 1;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            notifyCheckpointCounter++;
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }

    private static class StatefulMapFunctionWithAsyncOperateRetriableException
            extends RichMapFunction<Tuple2<Integer, Long>, Tuple2<Integer, Long>>
            implements CheckpointedFunction, CheckpointListener {

        private long checkpointID;

        private ListState<Long> listState;

        private long completedCheckpointLimit;

        private long notifyCheckpointCounter;

        private static int[] asyncOperateCounter = new int[NUM_TASK_MANAGERS * NUM_TASK_SLOTS];

        public StatefulMapFunctionWithAsyncOperateRetriableException(
                long completedCheckpointLimit) {
            this.completedCheckpointLimit = completedCheckpointLimit;
        }

        @Override
        public Tuple2<Integer, Long> map(Tuple2<Integer, Long> value) throws Exception {
            return new Tuple2<>(value.f0, value.f1 + checkpointID);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            if (context.getCheckpointId() > completedCheckpointLimit) {
                throw new SuccessException();
            }
            listState.add(context.getCheckpointId());
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            listState =
                    context.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("checkpoint-01", Types.LONG));

            Iterator<Long> iterator = listState.get().iterator();
            if (iterator.hasNext()) {
                this.checkpointID = iterator.next();
            }
        }

        @Override
        public void asyncOperate(FunctionSnapshotContext context) throws Exception {
            if (context.getCheckpointId() == (completedCheckpointLimit / 2 + 1)) {
                throw new RetriableAsyncOperateException("asyncOperate error");
            }
            Thread.sleep(100);
            int index = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            asyncOperateCounter[index] = asyncOperateCounter[index] + 1;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            notifyCheckpointCounter++;
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }
}
