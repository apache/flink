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
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;
import org.apache.flink.runtime.testutils.InternalMiniClusterExtension;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.SerializedValue;

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterators;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration Test case that validates the exactly-once mechanism for coordinator events around
 * checkpoints. The test checks for two distinct problems related to exactly-once event delivery:
 *
 * <h2>1. Delayed events</h2>
 *
 * <p>When the OperatorCoordinator runs in its own thread (which they commonly do), it is possible
 * that races occur between when an event is meant to be sent, and when it actually gets sent, and
 * the notifications about task failures.
 *
 * <p>For example, an event that was meant to target task-execution-attempt X might actually get
 * sent when task-execution-attempt X+1 is already running. If the coordinator has not yet processed
 * the information that task-execution-attempt X is no longer running, and that
 * task-execution-attempt X+1 is now running, then we don't want events to sneakily reach
 * task-execution-attempt X+1. Otherwise we cannot reason about which events need to be resent
 * because of the failure, and which do not.
 *
 * <p>So this test checks the following condition: After a task has failed over to a new execution,
 * events being sent must not reach that new task before the notification has reached the
 * coordinator about the previous task failure and the new task execution.
 *
 * <h2>2. Exactly-once alignment between multiple Coordinators</h2>
 *
 * <p>After a coordinator completed its checkpoint future, all events sent after that must be held
 * back until its subtasks completed their checkpoint. That is because from the coordinator's
 * perspective, the events are after the checkpoint, so they must also be after the checkpoint from
 * the subtask's perspective.
 *
 * <p>When multiple coordinators exist, there are time spans during which some coordinators
 * completed their checkpoints, but others did not yet, and hence the source checkpoint barriers are
 * not yet injected (that happens only once all coordinators are done with their checkpoint). The
 * events from the earlier coordinators must be blocked until all coordinators complete their
 * checkpoints, the source checkpoint barriers are injected, and their subtasks also complete the
 * current checkpoint.
 *
 * <p>The test generates two sequences of events form two Operator Coordinators to two operators
 * (tasks). The event sequences have a different speed in which they are sent. The coordinators have
 * different delays in which they complete their checkpoints. Both coordinators inject failures at
 * different points.
 */
public class CoordinatorEventsExactlyOnceITCase {

    private static final ConfigOption<String> ACC_NAME =
            ConfigOptions.key("acc").stringType().noDefaultValue();

    private static final String OPERATOR_1_NAME = "operator-1";
    private static final String OPERATOR_2_NAME = "operator-2";

    @RegisterExtension
    protected static final InternalMiniClusterExtension MINI_CLUSTER =
            new InternalMiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    // ------------------------------------------------------------------------

    @Test
    void test() throws Exception {
        // this captures variables communicated across instances, recoveries, etc.
        TestScript.reset();

        final int numEvents1 = 200;
        final int numEvents2 = 10;
        final int delay1 = 1;
        final int delay2 = 100;

        final JobVertex task1 = buildJobVertex(OPERATOR_1_NAME, numEvents1, delay1);
        final JobVertex task2 = buildJobVertex(OPERATOR_2_NAME, numEvents2, delay2);

        final JobGraph jobGraph =
                JobGraphBuilder.newStreamingJobGraphBuilder()
                        .setJobName("Coordinator Events Job")
                        .addJobVertices(Arrays.asList(task1, task2))
                        .setJobCheckpointingSettings(createCheckpointSettings())
                        .build();

        final JobExecutionResult result =
                MINI_CLUSTER.getMiniCluster().executeJobBlocking(jobGraph);

        checkListContainsSequence(result.getAccumulatorResult(OPERATOR_1_NAME), numEvents1);
        checkListContainsSequence(result.getAccumulatorResult(OPERATOR_2_NAME), numEvents2);
    }

    protected static void checkListContainsSequence(List<Integer> ints, int length) {
        Integer[] expected = new Integer[length];
        for (int i = 0; i < length; i++) {
            expected[i] = i;
        }
        assertThat(ints).containsExactly(expected);
    }

    // ------------------------------------------------------------------------
    //  test setup helpers
    // ------------------------------------------------------------------------

    private static JobVertex buildJobVertex(String name, int numEvents, int delay)
            throws IOException {
        final JobVertex vertex = new JobVertex(name);
        final OperatorID opId = OperatorID.fromJobVertexID(vertex.getID());

        vertex.setParallelism(1);
        vertex.setInvokableClass(EventCollectingTask.class);
        vertex.getConfiguration().setString(ACC_NAME, name);

        final OperatorCoordinator.Provider provider =
                new OperatorCoordinator.Provider() {

                    @Override
                    public OperatorID getOperatorId() {
                        return opId;
                    }

                    @Override
                    public OperatorCoordinator create(OperatorCoordinator.Context context) {
                        return new EventSendingCoordinator(context, name, numEvents, delay);
                    }
                };

        vertex.addOperatorCoordinator(new SerializedValue<>(provider));

        return vertex;
    }

    private static JobCheckpointingSettings createCheckpointSettings() {
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

    /**
     * An operator event to notify the coordinator that the test subtask is ready to accept events.
     */
    protected static final class StartEvent implements OperatorEvent {

        /**
         * The last integer value the subtask has received from the coordinator and stored in
         * snapshot, or -1 if the subtask has not completed any checkpoint yet.
         */
        private final int lastValue;

        public StartEvent(int lastValue) {
            this.lastValue = lastValue;
        }
    }

    protected static final class EndEvent implements OperatorEvent {}

    protected static final class IntegerEvent implements OperatorEvent {

        public final int value;

        private IntegerEvent(int value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "IntegerEvent " + value;
        }
    }

    private static final class IntegerRequest implements CoordinationRequest {
        final int value;

        private IntegerRequest(int value) {
            this.value = value;
        }
    }

    private static final class IntegerResponse implements CoordinationResponse {
        final int value;

        private IntegerResponse(int value) {
            this.value = value;
        }
    }

    // ------------------------------------------------------------------------

    /**
     * The coordinator that sends events and completes checkpoints.
     *
     * <p>All consistency guaranteed for the coordinator apply to order or method invocations (like
     * {@link #executionAttemptFailed(int, int, Throwable)}}, {@link #subtaskReset(int, long)} or
     * {@link #checkpointCoordinator(long, CompletableFuture)}) and the order in which actions are
     * done (sending events and completing checkpoints). Tho consistently evaluate this, but with
     * concurrency against the scheduler thread that calls this coordinator implements a simple
     * mailbox that moves the method handling into a separate thread, but keeps the order.
     *
     * <p>It would inject a failure at some point while sending out operator events. This behavior
     * helps to trigger a fail-over of the Flink job and test the exactly-once of events delivery in
     * this case.
     */
    protected static class EventSendingCoordinator
            implements OperatorCoordinator, CoordinationRequestHandler {

        protected final Context context;

        /** The max number that the coordinator might send out before it injects the failure. */
        protected final int maxNumberBeforeFailure;

        /**
         * This contains all variables that are necessary to track the progress of the test, and
         * which need to be tracked across instances of this coordinator (some scheduler
         * implementations may re-instantiate the ExecutionGraph and the coordinators around global
         * failures).
         */
        protected final TestScript testScript;

        private final ExecutorService mailboxExecutor;
        private final ScheduledExecutorService scheduledExecutor;

        private final int delay;
        private final int maxNumber;

        protected int nextNumber;

        protected CompletableFuture<byte[]> nextToComplete;
        protected CompletableFuture<byte[]> requestedCheckpoint;

        private SubtaskGateway subtaskGateway;
        private boolean workLoopRunning;

        protected EventSendingCoordinator(Context context, String name, int numEvents, int delay) {
            checkArgument(delay > 0);
            checkArgument(numEvents >= 3);

            this.context = context;
            this.maxNumber = numEvents;
            this.delay = delay;

            this.testScript = TestScript.getForOperator(name);

            this.mailboxExecutor =
                    Executors.newSingleThreadExecutor(
                            new DispatcherThreadFactory(
                                    Thread.currentThread().getThreadGroup(),
                                    "Coordinator Mailbox for " + name));
            this.scheduledExecutor =
                    Executors.newSingleThreadScheduledExecutor(
                            new DispatcherThreadFactory(
                                    Thread.currentThread().getThreadGroup(),
                                    "Coordinator Periodic Actions for " + name));

            this.nextNumber = 0;
            this.maxNumberBeforeFailure = numEvents * 2 / 3 + new Random().nextInt(numEvents / 6);
        }

        @Override
        public void start() throws Exception {}

        @Override
        public void close() throws Exception {
            scheduledExecutor.shutdownNow();
            assertThat(scheduledExecutor.awaitTermination(10, TimeUnit.MINUTES)).isTrue();

            mailboxExecutor.shutdownNow();
            assertThat(mailboxExecutor.awaitTermination(10, TimeUnit.MINUTES)).isTrue();
        }

        @Override
        public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event)
                throws Exception {
            if (subtask != 0 || !(event instanceof StartEvent)) {
                throw new Exception(
                        String.format("Don't recognize event '%s' from task %d.", event, subtask));
            }

            // this unblocks all the delayed actions that where kicked off while the previous
            // task was still running (if there was a previous task). this is part of simulating
            // the extreme race where the coordinator thread stalls for so long that a new
            // task execution attempt gets deployed before the last events targeted at the old task
            // where sent.
            testScript.signalRecoveredTaskReady();

            // first, we hand this over to the mailbox thread, so we preserve order on operations,
            // even if the action is only to do a thread safe scheduling into the scheduledExecutor
            runInMailbox(
                    () -> {
                        checkState(!workLoopRunning);
                        checkState(subtaskGateway != null);

                        if (((StartEvent) event).lastValue >= 0) {
                            nextNumber = ((StartEvent) event).lastValue + 1;
                        }

                        workLoopRunning = true;
                        scheduleSingleAction();
                    });
        }

        @Override
        public void executionAttemptFailed(
                int subtask, int attemptNumber, @Nullable Throwable reason) {
            // we need to create and register this outside the mailbox so that the
            // registration is not affected by the artificial stall on the mailbox, but happens
            // strictly before the tasks are restored and the operator events are received (to
            // trigger the latches) which also happens outside the mailbox.

            final CountDownLatch successorIsRunning = new CountDownLatch(1);
            testScript.registerHookToNotifyAfterTaskRecovered(successorIsRunning);

            // simulate a heavy thread race here: the mailbox has a last enqueued action before the
            // cancellation is processed. But through a race, the mailbox freezes for a while and in
            // that time, the task already went through a recovery cycle. By the time the mailbox
            // unfreezes, the new task will be the recipient of new events.
            // to simulate this race, we wait precisely until the point when the new task pings the
            // coordinator before unfreezing the mailbox
            runInMailbox(
                    () -> {
                        try {
                            successorIsRunning.await();
                        } catch (Exception ignored) {
                        }

                        executeSingleAction();
                    });

            // after the late racing action, this is the proper shutdown
            runInMailbox(
                    () -> {
                        workLoopRunning = false;
                        subtaskGateway = null;
                    });
        }

        @Override
        public void subtaskReset(int subtask, long checkpointId) {}

        @Override
        public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {
            runInMailbox(
                    () -> {
                        checkState(!workLoopRunning);
                        subtaskGateway = gateway;
                    });
        }

        @Override
        public void resetToCheckpoint(
                final long checkpointId, @Nullable final byte[] checkpointData) throws Exception {
            runInMailbox(
                    () -> nextNumber = checkpointData == null ? 0 : bytesToInt(checkpointData));
        }

        @Override
        public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result)
                throws Exception {
            runInMailbox(() -> requestedCheckpoint = result);
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}

        protected void runInMailbox(Runnable action) {
            mailboxExecutor.execute(
                    () -> {
                        try {
                            action.run();
                        } catch (Throwable t) {
                            // this eventually kills the test, which is harsh but the simplest way
                            // to make sure exceptions that bubble up are not swallowed and hide
                            // problems. To simplify debugging, we print the stack trace here before
                            // the exception
                            t.printStackTrace();
                            ExceptionUtils.rethrow(t);
                        }
                    });
        }

        void scheduleSingleAction() {
            try {
                scheduledExecutor.schedule(
                        () -> runInMailbox(this::executeSingleAction),
                        delay,
                        TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException e) {
                if (!scheduledExecutor.isShutdown()) {
                    throw e;
                }
            }
        }

        @SuppressWarnings("CallToPrintStackTrace")
        private void executeSingleAction() {
            if (!workLoopRunning) {
                // if the delay scheduler put a task in here, but we really aren't
                // working any more, then skip this
                return;
            }

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

            // schedule the next step. we do this here, after the previous step concluded, rather
            // than scheduling a periodic action. Otherwise, the periodic task would enqueue many
            // actions while the mailbox stalls and process them all instantaneously after the
            // un-stalling. That wouldn't break the test, but it voids the differences in event
            // sending delays between the different coordinators, which are part of provoking the
            // situation that requires checkpoint alignment between the coordinators' event streams.
            scheduleSingleAction();
        }

        protected void handleCheckpoint() {
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

        protected void sendNextEvent() {
            if (nextNumber > maxNumber) {
                return;
            }

            if (nextNumber == maxNumber) {
                subtaskGateway.sendEvent(new EndEvent());
            } else {
                subtaskGateway.sendEvent(new IntegerEvent(nextNumber));
            }

            nextNumber++;
        }

        private void checkWhetherToTriggerFailure() {
            if (nextNumber > maxNumberBeforeFailure && !testScript.hasAlreadyFailed()) {
                testScript.recordHasFailed();
                context.failJob(new Exception("test failure"));
            }
        }

        @Override
        public CompletableFuture<CoordinationResponse> handleCoordinationRequest(
                CoordinationRequest request) {
            if (request instanceof IntegerRequest) {
                int value = ((IntegerRequest) request).value;
                return CompletableFuture.completedFuture(new IntegerResponse(value + 1));
            } else {
                throw new UnsupportedOperationException("Unsupported request type: " + request);
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
                            operatorID, new SerializedValue<>(new StartEvent(-1)));

            // verify the request & response communication
            CoordinationResponse response =
                    getEnvironment()
                            .getOperatorCoordinatorEventGateway()
                            .sendRequestToCoordinator(
                                    operatorID, new SerializedValue<>(new IntegerRequest(100)))
                            .get();

            assertThat(response).isInstanceOf(IntegerResponse.class);
            assertThat(((IntegerResponse) response).value).isEqualTo(101);

            // poor-man's mailbox
            Object next;
            while (running && !((next = actions.take()) instanceof EndEvent)) {
                if (next instanceof IntegerEvent) {
                    collectedInts.add(((IntegerEvent) next).value);
                } else if (next instanceof CheckpointMetaData) {
                    takeCheckpoint(((CheckpointMetaData) next).getCheckpointId(), collectedInts);
                    getEnvironment()
                            .getOperatorCoordinatorEventGateway()
                            .sendOperatorEventToCoordinator(
                                    operatorID,
                                    new SerializedValue<>(
                                            new AcknowledgeCheckpointEvent(
                                                    ((CheckpointMetaData) next)
                                                            .getCheckpointId())));
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
        public CompletableFuture<Boolean> triggerCheckpointAsync(
                CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {
            actions.add(checkpointMetaData); // this signals the main thread should do a checkpoint
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public Future<Void> notifyCheckpointCompleteAsync(long checkpointId) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public Future<Void> notifyCheckpointAbortAsync(
                long checkpointId, long latestCompletedCheckpointId) {
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
    //  dedicated class to hold the "test script"
    // ------------------------------------------------------------------------

    protected static final class TestScript {

        private static final Map<String, TestScript> MAP_FOR_OPERATOR = new HashMap<>();

        public static TestScript getForOperator(String operatorName) {
            return MAP_FOR_OPERATOR.computeIfAbsent(operatorName, (key) -> new TestScript());
        }

        public static void reset() {
            MAP_FOR_OPERATOR.clear();
        }

        private final Collection<CountDownLatch> recoveredTaskRunning = new ArrayList<>();
        private boolean failedBefore;

        public void recordHasFailed() {
            this.failedBefore = true;
        }

        public boolean hasAlreadyFailed() {
            return failedBefore;
        }

        void registerHookToNotifyAfterTaskRecovered(CountDownLatch latch) {
            synchronized (recoveredTaskRunning) {
                recoveredTaskRunning.add(latch);
            }
        }

        void signalRecoveredTaskReady() {
            // We complete all latches that were registered. We may need to complete
            // multiple ones here, because it can happen that after a previous failure, the next
            // executions fails immediately again, before even registering at the coordinator.
            // in that case, we have multiple latches from multiple failure notifications waiting
            // to be completed.
            synchronized (recoveredTaskRunning) {
                for (CountDownLatch latch : recoveredTaskRunning) {
                    latch.countDown();
                }
                recoveredTaskRunning.clear();
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
        assertThat(bytes).hasSize(4);
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
