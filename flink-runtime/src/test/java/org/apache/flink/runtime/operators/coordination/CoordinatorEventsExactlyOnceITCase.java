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

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.PrioritizedOperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterators;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Integration Test case that validates the exactly-once mechanism for coordinator events around
 * checkpoints.
 *
 * <p>The test provokes the corner cases of the mechanism described in {@link
 * OperatorCoordinatorHolder}.
 *
 * <pre>
 * Coordinator one events: => a . . b . |trigger| . . |complete| . . c . . d . |barrier| . e . f
 * Coordinator two events: => . . x . . |trigger| . . . . . . . . . .|complete||barrier| . . y . . z
 * </pre>
 *
 * <p>The test generates two sequences of events form two Operator Coordinators to two operators
 * (tasks). The event sequences have a different speed in which they are sent. The coordinators have
 * different delays in which they complete their checkpoints. Both coordinators inject failures at
 * different points.
 */
@SuppressWarnings("serial")
public class CoordinatorEventsExactlyOnceITCase extends TestLogger {

    private static final ConfigOption<String> ACC_NAME =
            ConfigOptions.key("acc").stringType().noDefaultValue();
    private static final String OPERATOR_1_ACCUMULATOR = "op-acc-1";
    private static final String OPERATOR_2_ACCUMULATOR = "op-acc-2";

    private static MiniCluster miniCluster;

    @BeforeClass
    public static void startMiniCluster() throws Exception {
        final Configuration config = new Configuration();
        config.setString(RestOptions.BIND_PORT, "0");

        final MiniClusterConfiguration clusterCfg =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(2)
                        .setNumSlotsPerTaskManager(1)
                        .setConfiguration(config)
                        .build();

        miniCluster = new MiniCluster(clusterCfg);
        miniCluster.start();
    }

    @AfterClass
    public static void shutdownMiniCluster() throws Exception {
        miniCluster.close();
    }

    // ------------------------------------------------------------------------

    @Test
    @Ignore
    public void test() throws Exception {
        final int numEvents1 = 200;
        final int numEvents2 = 5;
        final int delay1 = 1;
        final int delay2 = 200;

        final JobVertex task1 =
                buildJobVertex("TASK_1", numEvents1, delay1, OPERATOR_1_ACCUMULATOR);
        final JobVertex task2 =
                buildJobVertex("TASK_2", numEvents2, delay2, OPERATOR_2_ACCUMULATOR);

        final JobGraph jobGraph =
                JobGraphBuilder.newStreamingJobGraphBuilder()
                        .setJobName("Coordinator Events Job")
                        .addJobVertices(Arrays.asList(task1, task2))
                        .setJobCheckpointingSettings(createCheckpointSettings(task1, task2))
                        .build();

        final JobExecutionResult result = miniCluster.executeJobBlocking(jobGraph);

        checkListContainsSequence(result.getAccumulatorResult(OPERATOR_2_ACCUMULATOR), numEvents2);
        checkListContainsSequence(result.getAccumulatorResult(OPERATOR_1_ACCUMULATOR), numEvents1);
    }

    private static void checkListContainsSequence(List<Integer> ints, int length) {
        if (ints.size() != length) {
            failList(ints, length);
        }

        int nextExpected = 0;
        for (int next : ints) {
            if (next != nextExpected++) {
                failList(ints, length);
            }
        }
    }

    private static void failList(List<Integer> ints, int length) {
        fail("List did not contain expected sequence of " + length + " elements, but was: " + ints);
    }

    // ------------------------------------------------------------------------
    //  test setup helpers
    // ------------------------------------------------------------------------

    private static JobVertex buildJobVertex(String name, int numEvents, int delay, String accName)
            throws IOException {
        final JobVertex vertex = new JobVertex(name);
        final OperatorID opId = OperatorID.fromJobVertexID(vertex.getID());

        vertex.setParallelism(1);
        vertex.setInvokableClass(EventCollectingTask.class);
        vertex.getConfiguration().setString(ACC_NAME, accName);

        final OperatorCoordinator.Provider provider =
                new OperatorCoordinator.Provider() {

                    @Override
                    public OperatorID getOperatorId() {
                        return opId;
                    }

                    @Override
                    public OperatorCoordinator create(OperatorCoordinator.Context context) {
                        return new EventSendingCoordinator(context, numEvents, delay);
                    }
                };

        vertex.addOperatorCoordinator(new SerializedValue<>(provider));

        return vertex;
    }

    private static JobCheckpointingSettings createCheckpointSettings(JobVertex... vertices) {
        final CheckpointCoordinatorConfiguration coordCfg =
                new CheckpointCoordinatorConfiguration.CheckpointCoordinatorConfigurationBuilder()
                        .setMaxConcurrentCheckpoints(1)
                        .setCheckpointInterval(10)
                        .setCheckpointTimeout(100_000)
                        .build();

        return new JobCheckpointingSettings(coordCfg, null);
    }

    // ------------------------------------------------------------------------
    //  test operator and coordinator implementations
    // ------------------------------------------------------------------------

    private static final class StartEvent implements OperatorEvent {}

    private static final class EndEvent implements OperatorEvent {}

    private static final class IntegerEvent implements OperatorEvent {

        final int value;

        IntegerEvent(int value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "IntegerEvent " + value;
        }
    }

    // ------------------------------------------------------------------------

    private static final class EventSendingCoordinator implements OperatorCoordinator, Runnable {

        private final Context context;

        private ScheduledExecutorService executor;
        private volatile ScheduledFuture<?> periodicTask;

        private final int delay;
        private final int maxNumber;
        private int nextNumber;

        private volatile CompletableFuture<byte[]> requestedCheckpoint;
        private CompletableFuture<byte[]> nextToComplete;

        private final int failAtMessage;
        private boolean failedBefore;

        private EventSendingCoordinator(Context context, int numEvents, int delay) {
            checkArgument(delay > 0);
            checkArgument(numEvents >= 3);

            this.context = context;
            this.maxNumber = numEvents;
            this.delay = delay;
            this.executor = Executors.newSingleThreadScheduledExecutor();

            this.failAtMessage = numEvents / 3 + new Random().nextInt(numEvents / 3);
        }

        @Override
        public void start() throws Exception {}

        @Override
        public void close() throws Exception {
            executor.shutdownNow();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        @Override
        public void handleEventFromOperator(int subtask, OperatorEvent event) throws Exception {
            if (subtask != 0 || !(event instanceof StartEvent)) {
                throw new Exception(
                        String.format("Don't recognize event '%s' from task %d.", event, subtask));
            }

            if (periodicTask != null) {
                throw new Exception("periodic already running");
            }
            periodicTask =
                    executor.scheduleWithFixedDelay(this, delay, delay, TimeUnit.MILLISECONDS);
        }

        @Override
        public void subtaskFailed(int subtask, @Nullable Throwable reason) {
            periodicTask.cancel(false);
            periodicTask = null;
            executor.execute(() -> nextNumber = 0);
        }

        @Override
        public void subtaskReset(int subtask, long checkpointId) {}

        @Override
        public void resetToCheckpoint(
                final long checkpointId, @Nullable final byte[] checkpointData) throws Exception {
            executor.execute(() -> nextNumber = bytesToInt(checkpointData));
        }

        @Override
        public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result)
                throws Exception {
            requestedCheckpoint = result;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}

        @SuppressWarnings("CallToPrintStackTrace")
        @Override
        public void run() {
            try {
                handleCheckpoint();
                sendNextEvent();
                checkWhetherToTriggerFailure();
            } catch (Throwable t) {
                // this is so that exceptions thrown in the scheduled executor don't just freeze the
                // test
                t.printStackTrace();
                System.exit(-1);
            }
        }

        private void handleCheckpoint() {
            // we move the checkpoint one further so it completed after the next delay
            if (nextToComplete != null) {
                final int numToCheckpoint = Math.min(nextNumber, maxNumber);
                nextToComplete.complete(intToBytes(numToCheckpoint));
                nextToComplete = null;
            }
            if (requestedCheckpoint != null) {
                nextToComplete = requestedCheckpoint;
                requestedCheckpoint = null;
            }
        }

        private void sendNextEvent() {
            if (nextNumber > maxNumber) {
                return;
            }
            try {
                if (nextNumber == maxNumber) {
                    context.sendEvent(new EndEvent(), 0);
                } else {
                    context.sendEvent(new IntegerEvent(nextNumber), 0);
                }
                nextNumber++;
            } catch (TaskNotRunningException ignored) {
            }
        }

        private void checkWhetherToTriggerFailure() {
            if (nextNumber >= failAtMessage && !failedBefore) {
                failedBefore = true;
                context.failJob(new Exception("test failure"));
            }
        }
    }

    // ------------------------------------------------------------------------

    /**
     * The runtime task that receives the events and accumulates the numbers. The task is stateful
     * and checkpoints the accumulator.
     */
    public static final class EventCollectingTask extends AbstractInvokable {

        private final OperatorID operatorID;
        private final String accumulatorName;
        private final LinkedBlockingQueue<Object> actions;

        private volatile boolean running = true;

        public EventCollectingTask(Environment environment) {
            super(environment);
            this.operatorID = OperatorID.fromJobVertexID(environment.getJobVertexId());
            this.accumulatorName = environment.getTaskConfiguration().get(ACC_NAME);
            this.actions = new LinkedBlockingQueue<>();
        }

        @Override
        public void invoke() throws Exception {
            final ArrayList<Integer> collectedInts = new ArrayList<>();
            restoreState(collectedInts);

            // signal the coordinator to start
            getEnvironment()
                    .getOperatorCoordinatorEventGateway()
                    .sendOperatorEventToCoordinator(
                            operatorID, new SerializedValue<>(new StartEvent()));

            // poor-man's mailbox
            Object next;
            while (running && !((next = actions.take()) instanceof EndEvent)) {
                if (next instanceof IntegerEvent) {
                    collectedInts.add(((IntegerEvent) next).value);
                } else if (next instanceof CheckpointMetaData) {
                    takeCheckpoint(((CheckpointMetaData) next).getCheckpointId(), collectedInts);
                } else {
                    throw new Exception("Unrecognized: " + next);
                }
            }

            if (running) {
                final ListAccumulator<Integer> acc = new ListAccumulator<>();
                collectedInts.forEach(acc::add);
                getEnvironment().getAccumulatorRegistry().getUserMap().put(accumulatorName, acc);
            }
        }

        @Override
        public void cancel() throws Exception {
            running = false;
        }

        @Override
        public Future<Boolean> triggerCheckpointAsync(
                CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {
            actions.add(checkpointMetaData); // this signals the main thread should do a checkpoint
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public Future<Void> notifyCheckpointCompleteAsync(long checkpointId) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public Future<Void> notifyCheckpointAbortAsync(long checkpointId) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void dispatchOperatorEvent(OperatorID operator, SerializedValue<OperatorEvent> event)
                throws FlinkException {
            try {
                final OperatorEvent opEvent = event.deserializeValue(getUserCodeClassLoader());
                actions.add(opEvent);
            } catch (IOException | ClassNotFoundException e) {
                throw new FlinkException(e);
            }
        }

        private void takeCheckpoint(long checkpointId, List<Integer> state) throws Exception {
            final StreamStateHandle handle = stateToHandle(state);
            final TaskStateSnapshot snapshot = createSnapshot(handle, operatorID);
            getEnvironment().acknowledgeCheckpoint(checkpointId, new CheckpointMetrics(), snapshot);
        }

        private void restoreState(List<Integer> target) throws Exception {
            final StreamStateHandle stateHandle =
                    readSnapshot(getEnvironment().getTaskStateManager(), operatorID);
            if (stateHandle != null) {
                final List<Integer> list = handleToState(stateHandle);
                target.addAll(list);
            }
        }
    }

    // ------------------------------------------------------------------------
    //  serialization shenannigans
    // ------------------------------------------------------------------------

    static byte[] intToBytes(int value) {
        final byte[] bytes = new byte[4];
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).putInt(0, value);
        return bytes;
    }

    static int bytesToInt(byte[] bytes) {
        assertEquals(4, bytes.length);
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt(0);
    }

    static ByteStreamStateHandle stateToHandle(List<Integer> state) throws IOException {
        final byte[] bytes = InstantiationUtil.serializeObject(state);
        return new ByteStreamStateHandle("state", bytes);
    }

    static List<Integer> handleToState(StreamStateHandle handle)
            throws IOException, ClassNotFoundException {
        final ByteStreamStateHandle byteHandle = (ByteStreamStateHandle) handle;
        return InstantiationUtil.deserializeObject(
                byteHandle.getData(), EventCollectingTask.class.getClassLoader());
    }

    static TaskStateSnapshot createSnapshot(StreamStateHandle handle, OperatorID operatorId) {
        final OperatorStateHandle.StateMetaInfo metaInfo =
                new OperatorStateHandle.StateMetaInfo(
                        new long[] {0}, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE);

        final OperatorStateHandle state =
                new OperatorStreamStateHandle(
                        Collections.singletonMap("état_et_moi_:_ça_fait_deux", metaInfo), handle);

        final OperatorSubtaskState oss =
                OperatorSubtaskState.builder().setManagedOperatorState(state).build();
        return new TaskStateSnapshot(Collections.singletonMap(operatorId, oss));
    }

    @Nullable
    static StreamStateHandle readSnapshot(TaskStateManager stateManager, OperatorID operatorId) {
        final PrioritizedOperatorSubtaskState poss =
                stateManager.prioritizedOperatorState(operatorId);
        if (!poss.isRestored()) {
            return null;
        }

        final StateObjectCollection<OperatorStateHandle> opState =
                stateManager
                        .prioritizedOperatorState(operatorId)
                        .getPrioritizedManagedOperatorState()
                        .get(0);
        final OperatorStateHandle handle = Iterators.getOnlyElement(opState.iterator());
        return handle.getDelegateStateHandle();
    }
}
