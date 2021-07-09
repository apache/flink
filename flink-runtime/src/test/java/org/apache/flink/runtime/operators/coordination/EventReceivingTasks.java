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

import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.coordination.util.IncompleteFuturesTracker;
import org.apache.flink.util.SerializedValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * A test implementation of the BiFunction interface used as the underlying event sender in the
 * {@link OperatorCoordinatorHolder}.
 */
public class EventReceivingTasks implements SubtaskAccess.SubtaskAccessFactory {

    public static EventReceivingTasks createForNotYetRunningTasks() {
        return new EventReceivingTasks(false, CompletableFuture.completedFuture(Acknowledge.get()));
    }

    public static EventReceivingTasks createForRunningTasks() {
        return new EventReceivingTasks(true, CompletableFuture.completedFuture(Acknowledge.get()));
    }

    public static EventReceivingTasks createForRunningTasksFailingRpcs(Throwable rpcException) {
        return new EventReceivingTasks(true, FutureUtils.completedExceptionally(rpcException));
    }

    public static EventReceivingTasks createForRunningTasksWithRpcResult(
            CompletableFuture<Acknowledge> result) {
        return new EventReceivingTasks(true, result);
    }

    // ------------------------------------------------------------------------

    final ArrayList<EventWithSubtask> events = new ArrayList<>();

    private final CompletableFuture<Acknowledge> eventSendingResult;

    private final Map<Integer, TestSubtaskAccess> subtasks = new HashMap<>();

    private final boolean createdTasksAreRunning;

    private EventReceivingTasks(
            final boolean createdTasksAreRunning,
            final CompletableFuture<Acknowledge> eventSendingResult) {
        this.createdTasksAreRunning = createdTasksAreRunning;
        this.eventSendingResult = eventSendingResult;
    }

    // ------------------------------------------------------------------------
    //  Access to sent events
    // ------------------------------------------------------------------------

    public int getNumberOfSentEvents() {
        return events.size();
    }

    public List<EventWithSubtask> getAllSentEvents() {
        return events;
    }

    public List<OperatorEvent> getSentEventsForSubtask(int subtaskIndex) {
        return events.stream()
                .filter((evt) -> evt.subtask == subtaskIndex)
                .map((evt) -> evt.event)
                .collect(Collectors.toList());
    }

    // ------------------------------------------------------------------------
    //  Controlling the life cycle of the target tasks
    // ------------------------------------------------------------------------

    @Override
    public SubtaskAccess getAccessForSubtask(int subtask) {
        return subtasks.computeIfAbsent(
                subtask, (subtaskIdx) -> new TestSubtaskAccess(subtaskIdx, createdTasksAreRunning));
    }

    public OperatorCoordinator.SubtaskGateway createGatewayForSubtask(int subtask) {
        final SubtaskAccess sta = getAccessForSubtask(subtask);
        return new SubtaskGatewayImpl(
                sta,
                new OperatorEventValve(),
                Executors.directExecutor(),
                new IncompleteFuturesTracker());
    }

    public void switchTaskToRunning(int subtask) {
        final TestSubtaskAccess task = subtasks.get(subtask);
        if (task != null) {
            task.switchToRunning();
        } else {
            throw new IllegalArgumentException("No subtask created for " + subtask);
        }
    }

    public void switchAllTasksToRunning() {
        for (TestSubtaskAccess tsa : subtasks.values()) {
            tsa.switchToRunning();
        }
    }

    Callable<CompletableFuture<Acknowledge>> createSendAction(OperatorEvent event, int subtask) {
        return () -> {
            events.add(new EventWithSubtask(event, subtask));
            return eventSendingResult;
        };
    }

    // ------------------------------------------------------------------------

    /** A combination of an {@link OperatorEvent} and the target subtask it is sent to. */
    public static final class EventWithSubtask {

        public final OperatorEvent event;
        public final int subtask;

        public EventWithSubtask(OperatorEvent event, int subtask) {
            this.event = event;
            this.subtask = subtask;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final EventWithSubtask that = (EventWithSubtask) o;
            return subtask == that.subtask && event.equals(that.event);
        }

        @Override
        public int hashCode() {
            return Objects.hash(event, subtask);
        }

        @Override
        public String toString() {
            return event + " => subtask " + subtask;
        }
    }

    // ------------------------------------------------------------------------

    private final class TestSubtaskAccess implements SubtaskAccess {

        private final ExecutionAttemptID executionAttemptId = new ExecutionAttemptID();
        private final CompletableFuture<?> running;
        private final int subtaskIndex;

        private TestSubtaskAccess(int subtaskIndex, boolean isRunning) {
            this.subtaskIndex = subtaskIndex;
            this.running = new CompletableFuture<>();
            if (isRunning) {
                switchToRunning();
            }
        }

        @Override
        public Callable<CompletableFuture<Acknowledge>> createEventSendAction(
                SerializedValue<OperatorEvent> event) {

            final OperatorEvent deserializedEvent;
            try {
                deserializedEvent = event.deserializeValue(getClass().getClassLoader());
            } catch (IOException | ClassNotFoundException e) {
                throw new AssertionError(e);
            }

            return createSendAction(deserializedEvent, subtaskIndex);
        }

        @Override
        public int getSubtaskIndex() {
            return subtaskIndex;
        }

        @Override
        public ExecutionAttemptID currentAttempt() {
            return executionAttemptId;
        }

        @Override
        public String subtaskName() {
            return "test_task-" + subtaskIndex + " #: " + executionAttemptId;
        }

        @Override
        public CompletableFuture<?> hasSwitchedToRunning() {
            return running;
        }

        @Override
        public boolean isStillRunning() {
            return true;
        }

        void switchToRunning() {
            running.complete(null);
        }

        @Override
        public void triggerTaskFailover(Throwable cause) {
            // ignore this in the tests
        }
    }
}
