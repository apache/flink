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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.coordination.CoordinatorEventsExactlyOnceITCase;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
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
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test case that validates the exactly-once mechanism for operator events sent around
 * checkpoint. This class is an extension to {@link CoordinatorEventsExactlyOnceITCase}, further
 * verifying the exactly-once semantics of events in the following conditions:
 *
 * <h2>Stream operator recipient</h2>
 *
 * <p>In {@link CoordinatorEventsExactlyOnceITCase}, the test cases focus on verifying the
 * correctness of operator coordinator's behavior. It uses a custom {@link AbstractInvokable}
 * subclass that mocks the behavior of coordinator's recipients. This test class uses actual stream
 * operators as the recipient of coordinator events, verifying that stream operators can correctly
 * handle received operator events and inform coordinators of completing checkpoint.
 *
 * <h2>Non-source stream task</h2>
 *
 * <p>In {@link CoordinatorEventsExactlyOnceITCase}, the two tested tasks are executed independently
 * of each other. They do not have the upstream-downstream relationship as operators usually do in a
 * streaming job, and thus both of them are treated as source tasks. This test class further
 * verifies situations when the tested operators are not sources, which means when checkpoint
 * barriers are injected into sources, these operators may not have started checkpoint yet.
 *
 * <h2>Unaligned checkpoint</h2>
 *
 * <p>This class tests both aligned and unaligned checkpoints to verify that the correctness of the
 * event delivery behavior around checkpoint is not affected by this condition.
 *
 * <h2>Non-global failure</h2>
 *
 * <p>In {@link CoordinatorEventsExactlyOnceITCase}, failures occur at the coordinators' side, so
 * they will cause the whole Flink job to fail over. In this class, test cases are added when there
 * might only be fail-overs on the subtasks' side, while the coordinators are not affected. In this
 * case the production infrastructure code needs to work together with user code (implementations
 * inside the coordinator and operator subclass) to ensure the exactly-once semantics of operator
 * event delivery.
 *
 * <p>In the test cases of this class, the tested coordinator would inject failure during its
 * sending operator events. Besides, it is additionally guaranteed that there must have been a
 * checkpoint completed before the failure is injected, and that there must be events sent from the
 * coordinator to its subtask during checkpoint.
 */
public class CoordinatorEventsToStreamOperatorRecipientExactlyOnceITCase
        extends CoordinatorEventsExactlyOnceITCase {

    private static final int NUM_EVENTS = 100;

    private static final int DELAY = 10;

    private StreamExecutionEnvironment env;

    @BeforeEach
    void setup() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(100);
        ManuallyClosedSourceFunction.shouldCloseSource = false;
        EventReceivingOperator.shouldUnblockAllCheckpoint = false;
        EventReceivingOperator.shouldUnblockNextCheckpoint = false;
        TestScript.reset();
    }

    @Test
    void testCheckpoint() throws Exception {
        executeAndVerifyResults(
                env, new EventReceivingOperatorFactory<>("eventReceiving", NUM_EVENTS, DELAY));
    }

    @Test
    void testUnalignedCheckpoint() throws Exception {
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        executeAndVerifyResults(
                env, new EventReceivingOperatorFactory<>("eventReceiving", NUM_EVENTS, DELAY));
    }

    @Test
    void testCheckpointWithSubtaskFailure() throws Exception {
        executeAndVerifyResults(
                env,
                new EventReceivingOperatorWithFailureFactory<>(
                        "eventReceivingWithFailure", NUM_EVENTS, DELAY));
        assertThat(
                        TestScript.getForOperator("eventReceivingWithFailure-subtask0")
                                .hasAlreadyFailed())
                .isTrue();
    }

    @Test
    void testUnalignedCheckpointWithSubtaskFailure() throws Exception {
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        executeAndVerifyResults(
                env,
                new EventReceivingOperatorWithFailureFactory<>(
                        "eventReceivingWithFailure", NUM_EVENTS, DELAY));
        assertThat(
                        TestScript.getForOperator("eventReceivingWithFailure-subtask0")
                                .hasAlreadyFailed())
                .isTrue();
    }

    private void executeAndVerifyResults(
            StreamExecutionEnvironment env, EventReceivingOperatorFactory<Long, Long> factory)
            throws Exception {
        // The event receiving operator is not chained together with the source operator, so that
        // when checkpoint barriers are injected into sources, the event receiving operator has not
        // started checkpoint yet.
        env.addSource(new ManuallyClosedSourceFunction<>(), TypeInformation.of(Long.class))
                .disableChaining()
                .transform(factory.name, TypeInformation.of(Long.class), factory)
                .sinkTo(new DiscardingSink<>());

        JobExecutionResult executionResult =
                MINI_CLUSTER
                        .getMiniCluster()
                        .executeJobBlocking(env.getStreamGraph().getJobGraph());

        List<Integer> receivedInts =
                executionResult.getAccumulatorResult(EventReceivingOperator.ACCUMULATOR_NAME);
        checkListContainsSequence(receivedInts, NUM_EVENTS);
    }

    /** A mock source function that does not collect any stream record and closes on demand. */
    private static class ManuallyClosedSourceFunction<T> implements SourceFunction<T> {

        /** Whether the source function should be closed to finish the job. */
        private static boolean shouldCloseSource;

        @Override
        public void run(SourceContext<T> ctx) throws Exception {
            while (!shouldCloseSource) {
                Thread.sleep(100);
            }
        }

        @Override
        public void cancel() {}
    }

    /**
     * A wrapper operator factory for {@link EventSendingCoordinatorWithGuaranteedCheckpoint} and
     * {@link EventReceivingOperator}.
     */
    private static class EventReceivingOperatorFactory<IN, OUT>
            extends AbstractStreamOperatorFactory<OUT>
            implements CoordinatedOperatorFactory<OUT>, OneInputStreamOperatorFactory<IN, OUT> {

        protected final String name;

        protected final int numEvents;

        protected final int delay;

        public EventReceivingOperatorFactory(String name, int numEvents, int delay) {
            this.name = name;
            this.numEvents = numEvents;
            this.delay = delay;
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
                    return new EventSendingCoordinatorWithGuaranteedCheckpoint(
                            context, name, numEvents, delay);
                }
            };
        }

        @Override
        public <T extends StreamOperator<OUT>> T createStreamOperator(
                StreamOperatorParameters<OUT> parameters) {
            EventReceivingOperator<OUT> operator = new EventReceivingOperator<>();
            operator.setup(
                    parameters.getContainingTask(),
                    parameters.getStreamConfig(),
                    parameters.getOutput());
            parameters
                    .getOperatorEventDispatcher()
                    .registerEventHandler(parameters.getStreamConfig().getOperatorID(), operator);
            return (T) operator;
        }

        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return EventReceivingOperator.class;
        }
    }

    /**
     * A subclass of {@link EventSendingCoordinator} that additionally guarantees the following
     * behavior around checkpoint.
     *
     * <ul>
     *   <li>The job must have completed a checkpoint before the coordinator injects the failure.
     *   <li>The failure must be injected after the coordinator has completed its first checkpoint
     *       and before it completes the second.
     *   <li>There must be events being sent when the coordinator has completed the first checkpoint
     *       while the subtask has not.
     * </ul>
     */
    private static class EventSendingCoordinatorWithGuaranteedCheckpoint
            extends EventSendingCoordinator {

        /**
         * The max number that the coordinator might send out before it completes the first
         * checkpoint.
         */
        private final int maxNumberBeforeFirstCheckpoint;

        /** Whether the coordinator has sent any event to its subtask after any checkpoint. */
        private boolean isEventSentAfterFirstCheckpoint;

        /** Whether the coordinator has completed the first checkpoint. */
        private boolean isCoordinatorFirstCheckpointCompleted;

        /** Whether the job (both coordinator and operator) has completed the first checkpoint. */
        private boolean isJobFirstCheckpointCompleted;

        public EventSendingCoordinatorWithGuaranteedCheckpoint(
                Context context, String name, int numEvents, int delay) {
            super(context, name, numEvents, delay);
            this.maxNumberBeforeFirstCheckpoint = new Random().nextInt(numEvents / 6);
            this.isEventSentAfterFirstCheckpoint = false;
            this.isCoordinatorFirstCheckpointCompleted = false;
            this.isJobFirstCheckpointCompleted = false;
        }

        @Override
        protected void sendNextEvent() {
            if (!isCoordinatorFirstCheckpointCompleted
                    && nextNumber > maxNumberBeforeFirstCheckpoint) {
                return;
            }

            if (!isJobFirstCheckpointCompleted && nextNumber >= maxNumberBeforeFailure) {
                return;
            }

            super.sendNextEvent();

            if (!isEventSentAfterFirstCheckpoint && isCoordinatorFirstCheckpointCompleted) {
                isEventSentAfterFirstCheckpoint = true;
                EventReceivingOperator.shouldUnblockAllCheckpoint = true;
            }
        }

        @Override
        protected void handleCheckpoint() {
            if (nextToComplete != null) {
                isCoordinatorFirstCheckpointCompleted = true;
            }

            super.handleCheckpoint();

            if (nextToComplete != null
                    && isEventSentAfterFirstCheckpoint
                    && !testScript.hasAlreadyFailed()) {
                testScript.recordHasFailed();
                context.failJob(new Exception("test failure"));
            }
        }

        @Override
        public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData)
                throws Exception {
            super.resetToCheckpoint(checkpointId, checkpointData);
            runInMailbox(
                    () -> {
                        isCoordinatorFirstCheckpointCompleted = true;
                        isJobFirstCheckpointCompleted = true;
                    });
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) {
            super.notifyCheckpointAborted(checkpointId);
            runInMailbox(
                    () -> {
                        if (!isJobFirstCheckpointCompleted) {
                            isCoordinatorFirstCheckpointCompleted = false;
                        }
                    });
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            super.notifyCheckpointComplete(checkpointId);
            runInMailbox(() -> isJobFirstCheckpointCompleted = true);
        }
    }

    /**
     * The stream operator that receives the events and accumulates the numbers. The task is
     * stateful and checkpoints the accumulator.
     *
     * <p>The operator also supports blocking the checkpoint process until certain signal is invoked
     * (See {@link #shouldUnblockAllCheckpoint} and {@link #shouldUnblockNextCheckpoint}). It helps
     * to guarantee that there are events being sent when the coordinator has completed a checkpoint
     * while the subtask has not yet.
     */
    private static class EventReceivingOperator<T> extends AbstractStreamOperator<T>
            implements OneInputStreamOperator<T, T>, OperatorEventHandler {

        protected static final String ACCUMULATOR_NAME = "receivedIntegers";

        /** Whether to unblock all the following checkpoints. */
        private static boolean shouldUnblockAllCheckpoint;

        /** Whether to unblock the next checkpoint. */
        private static boolean shouldUnblockNextCheckpoint;

        protected final ListAccumulator<Integer> accumulator = new ListAccumulator<>();

        protected ListState<Integer> state;

        @Override
        public void open() throws Exception {
            super.open();
            getRuntimeContext().addAccumulator(ACCUMULATOR_NAME, accumulator);
        }

        @Override
        public void processElement(StreamRecord<T> element) throws Exception {
            // In test cases relevant to this class, there should be no stream record coming from
            // upstream operators. If this method is triggered, it means there might be a bug
            // somewhere in the production or test code, and thus an exception should be explicitly
            // thrown to reveal this situation.
            throw new UnsupportedOperationException();
        }

        @Override
        public void handleOperatorEvent(OperatorEvent evt) {
            if (evt instanceof IntegerEvent) {
                accumulator.add(((IntegerEvent) evt).value);
            } else if (evt instanceof EndEvent) {
                try {
                    state.update(accumulator.getLocalValue());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                ManuallyClosedSourceFunction.shouldCloseSource = true;
            } else {
                throw new UnsupportedOperationException();
            }
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            super.snapshotState(context);
            while (!shouldUnblockAllCheckpoint && !shouldUnblockNextCheckpoint) {
                Thread.sleep(100);
            }

            if (shouldUnblockNextCheckpoint) {
                shouldUnblockNextCheckpoint = false;
            }

            state.update(accumulator.getLocalValue());
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);

            state =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "accumulatorState", BasicTypeInfo.INT_TYPE_INFO));

            accumulator.resetLocal();
            state.get().forEach(accumulator::add);

            sendStartEvent();
        }

        protected void sendStartEvent() throws IOException {
            // signal the coordinator to start
            getContainingTask()
                    .getEnvironment()
                    .getOperatorCoordinatorEventGateway()
                    .sendOperatorEventToCoordinator(
                            getOperatorID(), new SerializedValue<>(new StartEvent(-1)));
        }
    }

    /**
     * A wrapper operator factory for {@link EventSendingCoordinatorWithGuaranteedCheckpoint} and
     * {@link EventReceivingOperatorWithFailure}.
     */
    private static class EventReceivingOperatorWithFailureFactory<IN, OUT>
            extends EventReceivingOperatorFactory<IN, OUT> {
        public EventReceivingOperatorWithFailureFactory(String name, int numEvents, int delay) {
            super(name, numEvents, delay);
        }

        @Override
        public <T extends StreamOperator<OUT>> T createStreamOperator(
                StreamOperatorParameters<OUT> parameters) {
            EventReceivingOperator<OUT> operator =
                    new EventReceivingOperatorWithFailure<>(name, numEvents);
            operator.setup(
                    parameters.getContainingTask(),
                    parameters.getStreamConfig(),
                    parameters.getOutput());
            parameters
                    .getOperatorEventDispatcher()
                    .registerEventHandler(parameters.getStreamConfig().getOperatorID(), operator);
            return (T) operator;
        }
    }

    /**
     * A subclass of {@link EventReceivingOperator} whose subtask would fail after receiving a
     * number of events. It is required that the parallelism of this operator is 1 in order for it
     * to work correctly.
     */
    private static class EventReceivingOperatorWithFailure<T> extends EventReceivingOperator<T> {

        private final String name;

        private final int maxNumberBeforeFailure;

        private TestScript testScript;

        private EventReceivingOperatorWithFailure(String name, int numEvents) {
            this.name = name;
            this.maxNumberBeforeFailure = numEvents / 3 + new Random().nextInt(numEvents / 6);
        }

        @Override
        public void setup(
                StreamTask<?, ?> containingTask,
                StreamConfig config,
                Output<StreamRecord<T>> output) {
            super.setup(containingTask, config, output);
            Preconditions.checkState(containingTask.getIndexInSubtaskGroup() == 0);
            this.testScript = TestScript.getForOperator(name + "-subtask0");
        }

        @Override
        public void handleOperatorEvent(OperatorEvent evt) {
            if (evt instanceof IntegerEvent) {
                if (((IntegerEvent) evt).value > maxNumberBeforeFailure
                        && !testScript.hasAlreadyFailed()) {
                    testScript.recordHasFailed();
                    throw new RuntimeException();
                }
                accumulator.add(((IntegerEvent) evt).value);
            } else if (evt instanceof EndEvent) {
                try {
                    state.update(accumulator.getLocalValue());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                if (testScript.hasAlreadyFailed()) {
                    ManuallyClosedSourceFunction.shouldCloseSource = true;
                }
            } else {
                throw new UnsupportedOperationException();
            }
        }

        @Override
        protected void sendStartEvent() throws IOException {
            // In a non-global fail-over, the infrastructure code needs to work together with the
            // user code to ensure exactly-once semantics.
            int lastValue = -1;

            List<Integer> list = accumulator.getLocalValue();
            if (!list.isEmpty()) {
                lastValue = list.get(list.size() - 1);
            }

            getContainingTask()
                    .getEnvironment()
                    .getOperatorCoordinatorEventGateway()
                    .sendOperatorEventToCoordinator(
                            getOperatorID(), new SerializedValue<>(new StartEvent(lastValue)));
        }
    }
}
