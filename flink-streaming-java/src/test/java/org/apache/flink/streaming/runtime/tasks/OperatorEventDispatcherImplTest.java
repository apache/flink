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

import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.AcknowledgeCloseGatewayEvent;
import org.apache.flink.runtime.operators.coordination.CloseGatewayEvent;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OpenGatewayEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.operators.coordination.TestOperatorEvent;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.taskmanager.NoOpTaskOperatorEventGateway;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.operators.collect.utils.MockOperatorStateStore;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.SerializedValue;

import org.junit.Test;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RunnableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link OperatorEventDispatcherImpl}. */
public class OperatorEventDispatcherImplTest {
    private static final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    @Test
    public void testSendOperatorEvent() {
        MockTaskOperatorEventGateway taskOperatorEventGateway =
                new MockTaskOperatorEventGateway(classLoader);
        OperatorEventDispatcherImpl dispatcher =
                new OperatorEventDispatcherImpl(classLoader, taskOperatorEventGateway);
        OperatorID operatorID = new OperatorID();

        OperatorEventGateway gateway = dispatcher.getOperatorEventGateway(operatorID);
        gateway.sendEventToCoordinator(new TestOperatorEvent(0));

        assertThat(taskOperatorEventGateway.sentEventsMap).containsOnlyKeys(operatorID);
        assertThat(taskOperatorEventGateway.sentEventsMap.get(operatorID))
                .containsExactly(new TestOperatorEvent(0));
    }

    @Test
    public void testReceiveAndForwardOperatorEvent() throws Exception {
        MockOperatorEventHandler handler = new MockOperatorEventHandler();
        OperatorEventDispatcherImpl dispatcher =
                new OperatorEventDispatcherImpl(classLoader, new NoOpTaskOperatorEventGateway());
        OperatorID operatorID = new OperatorID();
        dispatcher.registerEventHandler(operatorID, handler);

        dispatcher.dispatchEventToHandlers(operatorID, serialize(new TestOperatorEvent(0)));

        assertThat(handler.receivedEvents).containsExactly(new TestOperatorEvent(0));
    }

    @Test
    public void testCloseGatewayOnCheckpoint() throws Exception {
        MockTaskOperatorEventGateway taskOperatorEventGateway =
                new MockTaskOperatorEventGateway(classLoader);
        OperatorEventDispatcherImpl dispatcher =
                new OperatorEventDispatcherImpl(classLoader, taskOperatorEventGateway);
        OperatorID operatorID = new OperatorID();

        OperatorEventGateway gateway = dispatcher.getOperatorEventGateway(operatorID);
        dispatcher.dispatchEventToHandlers(operatorID, serialize(new CloseGatewayEvent(0L, 0)));
        gateway.sendEventToCoordinator(new TestOperatorEvent(0));

        assertThat(taskOperatorEventGateway.sentEventsMap).containsOnlyKeys(operatorID);
        assertThat(taskOperatorEventGateway.sentEventsMap.get(operatorID))
                .containsExactly(new AcknowledgeCloseGatewayEvent(0L, 0));
    }

    @Test
    public void testReopenGatewayOnCompletedCheckpoint() throws Exception {
        MockTaskOperatorEventGateway taskOperatorEventGateway =
                new MockTaskOperatorEventGateway(classLoader);
        OperatorEventDispatcherImpl dispatcher =
                new OperatorEventDispatcherImpl(classLoader, taskOperatorEventGateway);
        OperatorID operatorID = new OperatorID();
        StreamOperator<?> operator = new MockStreamOperator<>(operatorID);

        OperatorEventGateway gateway = dispatcher.getOperatorEventGateway(operatorID);
        dispatcher.dispatchEventToHandlers(operatorID, serialize(new CloseGatewayEvent(0L, 0)));
        gateway.sendEventToCoordinator(new TestOperatorEvent(0));
        dispatcher.snapshotOperatorEventGatewayIfExists(operator);
        dispatcher.notifyOperatorSnapshotCreatedIfExists(operator, 0L);

        assertThat(taskOperatorEventGateway.sentEventsMap).containsOnlyKeys(operatorID);
        assertThat(taskOperatorEventGateway.sentEventsMap.get(operatorID))
                .containsExactly(
                        new AcknowledgeCloseGatewayEvent(0L, 0),
                        new OpenGatewayEvent(0L, 0),
                        new TestOperatorEvent(0));
    }

    @Test
    public void testReopenGatewayOnConcurrentCheckpoint() throws Exception {
        MockTaskOperatorEventGateway taskOperatorEventGateway =
                new MockTaskOperatorEventGateway(classLoader);
        OperatorEventDispatcherImpl dispatcher =
                new OperatorEventDispatcherImpl(classLoader, taskOperatorEventGateway);
        OperatorID operatorID = new OperatorID();
        StreamOperator<?> operator = new MockStreamOperator<>(operatorID);

        OperatorEventGateway gateway = dispatcher.getOperatorEventGateway(operatorID);
        dispatcher.dispatchEventToHandlers(operatorID, serialize(new CloseGatewayEvent(0L, 0)));
        gateway.sendEventToCoordinator(new TestOperatorEvent(0));
        dispatcher.dispatchEventToHandlers(operatorID, serialize(new CloseGatewayEvent(1L, 0)));
        gateway.sendEventToCoordinator(new TestOperatorEvent(1));
        dispatcher.snapshotOperatorEventGatewayIfExists(operator);
        dispatcher.notifyOperatorSnapshotCreatedIfExists(operator, 0L);

        assertThat(taskOperatorEventGateway.sentEventsMap).containsOnlyKeys(operatorID);
        assertThat(taskOperatorEventGateway.sentEventsMap.get(operatorID))
                .containsExactly(
                        new AcknowledgeCloseGatewayEvent(0L, 0),
                        new AcknowledgeCloseGatewayEvent(1L, 0),
                        new OpenGatewayEvent(0L, 0),
                        new TestOperatorEvent(0));

        dispatcher.snapshotOperatorEventGatewayIfExists(operator);
        dispatcher.notifyOperatorSnapshotCreatedIfExists(operator, 1L);

        assertThat(taskOperatorEventGateway.sentEventsMap).containsOnlyKeys(operatorID);
        assertThat(taskOperatorEventGateway.sentEventsMap.get(operatorID))
                .containsExactly(
                        new AcknowledgeCloseGatewayEvent(0L, 0),
                        new AcknowledgeCloseGatewayEvent(1L, 0),
                        new OpenGatewayEvent(0L, 0),
                        new TestOperatorEvent(0),
                        new OpenGatewayEvent(1L, 0),
                        new TestOperatorEvent(1));
    }

    @Test
    public void testReloadOperatorEventFromSnapshot() throws Exception {
        MockTaskOperatorEventGateway taskOperatorEventGateway =
                new MockTaskOperatorEventGateway(classLoader);
        OperatorEventDispatcherImpl dispatcher =
                new OperatorEventDispatcherImpl(classLoader, taskOperatorEventGateway);
        OperatorID operatorID = new OperatorID();
        StreamOperator<?> operator = new MockStreamOperator<>(operatorID);

        OperatorEventGateway gateway = dispatcher.getOperatorEventGateway(operatorID);
        dispatcher.dispatchEventToHandlers(operatorID, serialize(new CloseGatewayEvent(0L, 0)));
        gateway.sendEventToCoordinator(new TestOperatorEvent(0));
        dispatcher.dispatchEventToHandlers(operatorID, serialize(new CloseGatewayEvent(1L, 0)));
        gateway.sendEventToCoordinator(new TestOperatorEvent(1));
        dispatcher.snapshotOperatorEventGatewayIfExists(operator);
        dispatcher.notifyOperatorSnapshotCreatedIfExists(operator, 0L);

        taskOperatorEventGateway.sentEventsMap.clear();

        OperatorEventDispatcherImpl reloadedDispatcher =
                new OperatorEventDispatcherImpl(classLoader, taskOperatorEventGateway);
        reloadedDispatcher.getOperatorEventGateway(operatorID);
        reloadedDispatcher.initializeOperatorEventGatewayIfExists(operator);

        assertThat(taskOperatorEventGateway.sentEventsMap).containsOnlyKeys(operatorID);
        assertThat(taskOperatorEventGateway.sentEventsMap.get(operatorID))
                .containsExactly(new TestOperatorEvent(0), new TestOperatorEvent(1));
    }

    private static class MockStreamOperator<T> extends AbstractStreamOperator<T> {
        private final OperatorID operatorID;

        private final OperatorStateBackend operatorStateBackend;

        private MockStreamOperator(OperatorID operatorID) {
            this.operatorID = operatorID;
            this.operatorStateBackend = new MockOperatorStateBackend();
        }

        @Override
        public OperatorID getOperatorID() {
            return operatorID;
        }

        @Override
        public OperatorStateBackend getOperatorStateBackend() {
            return operatorStateBackend;
        }

        @Override
        public StreamingRuntimeContext getRuntimeContext() {
            return new MockStreamingRuntimeContext(true, 1, 0);
        }
    }

    private static class MockOperatorStateBackend extends MockOperatorStateStore
            implements OperatorStateBackend {

        @Override
        public void dispose() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Nonnull
        @Override
        public RunnableFuture<SnapshotResult<OperatorStateHandle>> snapshot(
                long checkpointId,
                long timestamp,
                @Nonnull CheckpointStreamFactory streamFactory,
                @Nonnull CheckpointOptions checkpointOptions)
                throws Exception {
            throw new UnsupportedOperationException();
        }
    }

    private static SerializedValue<OperatorEvent> serialize(OperatorEvent event) {
        final SerializedValue<OperatorEvent> serializedEvent;
        try {
            serializedEvent = new SerializedValue<>(event);
        } catch (IOException e) {
            // this is not a recoverable situation, so we wrap this in an
            // unchecked exception and let it bubble up
            throw new FlinkRuntimeException("Cannot serialize operator event", e);
        }
        return serializedEvent;
    }

    /** A class that monitors all events sent to OC by an {@link OperatorEventDispatcherImpl}. */
    private static class MockTaskOperatorEventGateway implements TaskOperatorEventGateway {
        private final ClassLoader classLoader;

        private final Map<OperatorID, List<OperatorEvent>> sentEventsMap;

        private MockTaskOperatorEventGateway(ClassLoader classLoader) {
            this.classLoader = classLoader;
            this.sentEventsMap = new HashMap<>();
        }

        @Override
        public void sendOperatorEventToCoordinator(
                OperatorID operator, SerializedValue<OperatorEvent> serializedEvent) {
            final OperatorEvent evt;
            try {
                evt = serializedEvent.deserializeValue(classLoader);
            } catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException("Could not deserialize operator event", e);
            }
            sentEventsMap.computeIfAbsent(operator, key -> new ArrayList<>()).add(evt);
        }

        @Override
        public CompletableFuture<CoordinationResponse> sendRequestToCoordinator(
                OperatorID operator, SerializedValue<CoordinationRequest> request) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * A class that monitors all events forwarded to an operator by an {@link
     * OperatorEventDispatcherImpl}.
     */
    private static class MockOperatorEventHandler implements OperatorEventHandler {
        private final List<OperatorEvent> receivedEvents;

        private MockOperatorEventHandler() {
            this.receivedEvents = new ArrayList<>();
        }

        @Override
        public void handleOperatorEvent(OperatorEvent evt) {
            receivedEvents.add(evt);
        }
    }
}
