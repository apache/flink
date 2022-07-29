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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.operators.coordination.CoordinationEventsExactlyOnceITCaseUtils.IntegerEvent;
import static org.apache.flink.runtime.operators.coordination.CoordinationEventsExactlyOnceITCaseUtils.TestScript;
import static org.apache.flink.runtime.operators.coordination.CoordinationEventsExactlyOnceITCaseUtils.checkListContainsSequence;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test case that validates the exactly-once mechanism for operator events sent from an
 * operator to its coordinator around checkpoint.
 *
 * <p>In the test cases provided in this class, a test stream operator would send operator events to
 * its coordinator while aligned or unaligned checkpointing is enabled. Some of these events would
 * be sent when the coordinator has completed a checkpoint, while the operator has not yet. The
 * coordinator or operator may inject failures at some time during the job's execution, and this
 * class verifies that the exactly-once semantics of the delivery of these events would not be
 * affected in these situations.
 *
 * <p>See also {@link CoordinatorEventsToStreamOperatorRecipientExactlyOnceITCase} for integration
 * tests about operator events sent in the reversed direction.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class StreamOperatorEventsExactlyOnceITCase {

    @ClassRule
    public static final MiniClusterResource MINI_CLUSTER =
            new MiniClusterResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    private static final int NUM_EVENTS = 100;

    private static final int DELAY = 1;

    private StreamExecutionEnvironment env;

    @Before
    public void setup() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(100);
        EventReceivingCoordinator.RECEIVED_INTEGERS.clear();
        TestScript.reset();
    }

    @Test
    public void testCheckpointWithCoordinatorFailure() throws Exception {
        executeAndVerifyResult(true, false);
    }

    @Test
    public void testUnalignedCheckpointWithCoordinatorFailure() throws Exception {
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        executeAndVerifyResult(true, false);
    }

    @Test
    public void testCheckpointWithSubtaskFailure() throws Exception {
        executeAndVerifyResult(false, true);
    }

    @Test
    public void testUnalignedCheckpointWithSubtaskFailure() throws Exception {
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        executeAndVerifyResult(false, true);
    }

    private void executeAndVerifyResult(
            boolean shouldCoordinatorFailAtSecondCheckpoint,
            boolean shouldOperatorFailAtSecondCheckpoint)
            throws Exception {
        env.addSource(new GuaranteeCheckpointSourceFunction(NUM_EVENTS, DELAY))
                .disableChaining()
                .transform(
                        "eventSending",
                        TypeInformation.of(Integer.class),
                        new EventSendingOperatorFactory(
                                shouldCoordinatorFailAtSecondCheckpoint,
                                shouldOperatorFailAtSecondCheckpoint))
                .addSink(new DiscardingSink<>());

        MINI_CLUSTER.getMiniCluster().executeJobBlocking(env.getStreamGraph().getJobGraph());

        checkListContainsSequence(EventReceivingCoordinator.RECEIVED_INTEGERS, NUM_EVENTS);

        assertThat(TestScript.getForOperator("EventReceivingCoordinator").hasAlreadyFailed())
                .isEqualTo(shouldCoordinatorFailAtSecondCheckpoint);
        assertThat(TestScript.getForOperator("EventSendingOperator-subtask0").hasAlreadyFailed())
                .isEqualTo(shouldOperatorFailAtSecondCheckpoint);
    }

    /**
     * A source function that generates a fixed number of output integers and guarantees that there
     * are at lease two checkpoints.
     */
    private static class GuaranteeCheckpointSourceFunction
            extends RichParallelSourceFunction<Integer> implements CheckpointedFunction {

        private final int maxNumber;

        private final int maxNumberBeforeFistCheckpoint;

        private final int maxNumberBeforeSecondCheckpoint;

        private final int delay;

        private final AtomicInteger nextNumber;

        private boolean isFirstCheckpointCompleted;

        private boolean isSecondCheckpointCompleted;

        private ListState<Integer> nextNumberState;

        private GuaranteeCheckpointSourceFunction(int maxNumber, int delay) {
            this.maxNumber = maxNumber;
            this.maxNumberBeforeFistCheckpoint = maxNumber / 3;
            this.maxNumberBeforeSecondCheckpoint = maxNumber * 2 / 3;
            this.delay = delay;
            this.isFirstCheckpointCompleted = false;
            this.isSecondCheckpointCompleted = false;
            this.nextNumber = new AtomicInteger(0);
        }

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (nextNumber.get() < maxNumber) {
                while ((nextNumber.get() > maxNumberBeforeFistCheckpoint
                                && !isFirstCheckpointCompleted)
                        || (nextNumber.get() > maxNumberBeforeSecondCheckpoint
                                && !isSecondCheckpointCompleted)) {
                    Thread.sleep(10);
                }
                Thread.sleep(delay);

                ctx.collect(nextNumber.getAndIncrement());
            }
        }

        @Override
        public void cancel() {}

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Integer> descriptor =
                    new ListStateDescriptor<>("nextNumber", Integer.class);

            nextNumberState = context.getOperatorStateStore().getListState(descriptor);

            Iterator<Integer> iterator = nextNumberState.get().iterator();
            if (iterator.hasNext()) {
                isFirstCheckpointCompleted = true;
                nextNumber.set(iterator.next());
            }
            assertThat(iterator.hasNext()).isFalse();
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            if (!isFirstCheckpointCompleted) {
                isFirstCheckpointCompleted = true;
            } else if (!isSecondCheckpointCompleted) {
                isSecondCheckpointCompleted = true;
            }

            nextNumberState.update(Collections.singletonList(nextNumber.get()));
        }
    }

    /**
     * A wrapper operator factory for {@link EventReceivingCoordinator} and {@link
     * EventSendingOperator}.
     */
    private static class EventSendingOperatorFactory extends AbstractStreamOperatorFactory<Integer>
            implements CoordinatedOperatorFactory<Integer>,
                    OneInputStreamOperatorFactory<Integer, Integer> {
        private final boolean shouldCoordinatorFailAtSecondCheckpoint;

        private final boolean shouldOperatorFailAtSecondCheckpoint;

        private EventSendingOperatorFactory(
                boolean shouldCoordinatorFailAtSecondCheckpoint,
                boolean shouldOperatorFailAtSecondCheckpoint) {
            this.shouldCoordinatorFailAtSecondCheckpoint = shouldCoordinatorFailAtSecondCheckpoint;
            this.shouldOperatorFailAtSecondCheckpoint = shouldOperatorFailAtSecondCheckpoint;
        }

        @Override
        public OperatorCoordinator.Provider getCoordinatorProvider(
                String operatorName, OperatorID operatorID) {
            return new OperatorCoordinator.Provider() {

                @Override
                public OperatorID getOperatorId() {
                    return operatorID;
                }

                @Override
                public OperatorCoordinator create(OperatorCoordinator.Context context) {
                    return new EventReceivingCoordinator(
                            context, operatorID, shouldCoordinatorFailAtSecondCheckpoint);
                }
            };
        }

        @Override
        public <T extends StreamOperator<Integer>> T createStreamOperator(
                StreamOperatorParameters<Integer> parameters) {
            final OperatorID operatorId = parameters.getStreamConfig().getOperatorID();
            OperatorEventGateway gateway =
                    parameters.getOperatorEventDispatcher().getOperatorEventGateway(operatorId);
            EventSendingOperator operator =
                    new EventSendingOperator(gateway, shouldOperatorFailAtSecondCheckpoint);
            operator.setup(
                    parameters.getContainingTask(),
                    parameters.getStreamConfig(),
                    parameters.getOutput());
            return (T) operator;
        }

        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return EventSendingOperator.class;
        }
    }

    /**
     * A coordinator that listens on received integer events. It would store received integers into
     * a global list and its snapshots.
     */
    private static class EventReceivingCoordinator implements OperatorCoordinator {

        /** A global list that records the integers that the test coordinator has received. */
        private static final List<Integer> RECEIVED_INTEGERS = new ArrayList<>();

        private final Context context;

        private final TestScript testScript;

        private final ExecutorService mailboxExecutor;

        private final Map<Long, List<Integer>> attemptedCheckpointValueMap;

        private final boolean shouldFailAtSecondCheckpoint;

        private boolean isFirstCheckpointCompleted;

        private boolean isSecondCheckpointCompleted;

        private EventReceivingCoordinator(
                Context context, OperatorID operatorID, boolean shouldFailAtSecondCheckpoint) {
            this.context = context;
            this.testScript = TestScript.getForOperator("EventReceivingCoordinator");
            this.shouldFailAtSecondCheckpoint = shouldFailAtSecondCheckpoint;
            this.isFirstCheckpointCompleted = false;
            this.isSecondCheckpointCompleted = false;

            this.attemptedCheckpointValueMap = new HashMap<>();

            this.mailboxExecutor =
                    Executors.newSingleThreadExecutor(
                            new DispatcherThreadFactory(
                                    Thread.currentThread().getThreadGroup(),
                                    "Coordinator Mailbox for " + operatorID));
        }

        @Override
        public void start() throws Exception {}

        @Override
        public void close() throws Exception {
            mailboxExecutor.shutdownNow();
            assertThat(mailboxExecutor.awaitTermination(10, TimeUnit.MINUTES)).isTrue();
        }

        @Override
        public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event)
                throws Exception {
            if (subtask != 0 || !(event instanceof IntegerEvent)) {
                throw new Exception(
                        String.format("Don't recognize event '%s' from task %d.", event, subtask));
            }

            runInMailbox(() -> RECEIVED_INTEGERS.add(((IntegerEvent) event).value));
        }

        @Override
        public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture)
                throws Exception {
            runInMailbox(
                    () -> {
                        if (isFirstCheckpointCompleted
                                && !isSecondCheckpointCompleted
                                && shouldFailAtSecondCheckpoint
                                && !testScript.hasAlreadyFailed()) {
                            testScript.recordHasFailed();
                            context.failJob(new Exception("test failure"));
                            resultFuture.completeExceptionally(new Exception("test failure"));
                            return;
                        }

                        attemptedCheckpointValueMap.put(
                                checkpointId, new ArrayList<>(RECEIVED_INTEGERS));

                        ByteBuffer byteBuffer = ByteBuffer.allocate(RECEIVED_INTEGERS.size() * 4);
                        for (int i : RECEIVED_INTEGERS) {
                            byteBuffer.putInt(i);
                        }
                        resultFuture.complete(byteBuffer.array());
                    });
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            runInMailbox(
                    () -> {
                        if (!isFirstCheckpointCompleted) {
                            isFirstCheckpointCompleted = true;
                        } else if (!isSecondCheckpointCompleted) {
                            isSecondCheckpointCompleted = true;
                        }
                    });
        }

        @Override
        public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData) {
            runInMailbox(
                    () -> {
                        isFirstCheckpointCompleted = true;

                        RECEIVED_INTEGERS.clear();

                        if (checkpointData == null) {
                            return;
                        }

                        ByteBuffer byteBuffer = ByteBuffer.wrap(checkpointData);
                        for (int i = 0; i < checkpointData.length / 4; i++) {
                            RECEIVED_INTEGERS.add(byteBuffer.getInt());
                        }

                        attemptedCheckpointValueMap.put(
                                checkpointId, new ArrayList<>(RECEIVED_INTEGERS));
                    });
        }

        @Override
        public void subtaskReset(int subtask, long checkpointId) {
            runInMailbox(
                    () -> {
                        RECEIVED_INTEGERS.clear();
                        RECEIVED_INTEGERS.addAll(attemptedCheckpointValueMap.get(checkpointId));
                    });
        }

        @Override
        public void executionAttemptFailed(
                int subtask, int attemptNumber, @Nullable Throwable reason) {}

        @Override
        public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {}

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
    }

    /**
     * A stream operator that sends an integer event to its coordinator during its first checkpoint,
     * and when it receives a stream record whose value is larger than that of previously sent
     * events.
     *
     * <p>It is guaranteed that there are events sent out from this operator when its coordinator
     * has completed the first checkpoint, while the operator has not yet.
     */
    private static class EventSendingOperator extends AbstractStreamOperator<Integer>
            implements OneInputStreamOperator<Integer, Integer> {

        private final OperatorEventGateway gateway;

        private final boolean shouldFailAtSecondCheckpoint;

        private TestScript testScript;

        private ListState<Integer> lastSentNumberState;

        private int lastSentNumber;

        private boolean isFirstCheckpointCompleted;

        private boolean isSecondCheckpointCompleted;

        private EventSendingOperator(
                OperatorEventGateway gateway, boolean shouldFailAtSecondCheckpoint) {
            this.gateway = gateway;
            this.shouldFailAtSecondCheckpoint = shouldFailAtSecondCheckpoint;
            this.lastSentNumber = -1;
            this.isFirstCheckpointCompleted = false;
            this.isSecondCheckpointCompleted = false;
        }

        @Override
        public void setup(
                StreamTask<?, ?> containingTask,
                StreamConfig config,
                Output<StreamRecord<Integer>> output) {
            super.setup(containingTask, config, output);
            Preconditions.checkState(containingTask.getIndexInSubtaskGroup() == 0);
            testScript = TestScript.getForOperator("EventSendingOperator-subtask0");
        }

        @Override
        public void processElement(StreamRecord<Integer> element) throws Exception {
            if (lastSentNumber < element.getValue()) {
                gateway.sendEventToCoordinator(new IntegerEvent(element.getValue()));
                lastSentNumber = element.getValue();
            }
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);

            ListStateDescriptor<Integer> descriptor =
                    new ListStateDescriptor<>("lastSentNumber", Integer.class);

            lastSentNumberState = context.getOperatorStateStore().getListState(descriptor);
            Iterator<Integer> iterator = lastSentNumberState.get().iterator();
            if (iterator.hasNext()) {
                lastSentNumber = iterator.next();
            }
            assertThat(iterator.hasNext()).isFalse();
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            super.snapshotState(context);

            if (!isFirstCheckpointCompleted) {
                lastSentNumber++;
                gateway.sendEventToCoordinator(new IntegerEvent(lastSentNumber));
                isFirstCheckpointCompleted = true;
            } else if (!isSecondCheckpointCompleted) {
                if (shouldFailAtSecondCheckpoint && !testScript.hasAlreadyFailed()) {
                    testScript.recordHasFailed();
                    throw new RuntimeException();
                }
                isSecondCheckpointCompleted = true;
            }

            lastSentNumberState.update(Collections.singletonList(lastSentNumber));
        }
    }
}
