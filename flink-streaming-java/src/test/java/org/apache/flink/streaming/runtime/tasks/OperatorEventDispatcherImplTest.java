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

import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.AcknowledgeCheckpointEvent;
import org.apache.flink.runtime.operators.coordination.AcknowledgeCloseGatewayEvent;
import org.apache.flink.runtime.operators.coordination.CloseGatewayEvent;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.operators.coordination.TestOperatorEvent;
import org.apache.flink.streaming.api.operators.collect.utils.MockOperatorStateStore;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.SerializedValue;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link OperatorEventDispatcherImpl}. */
public class OperatorEventDispatcherImplTest {
    private static final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    @Test
    public void testSendOperatorEvent() {
        DispatcherEventListener eventListener = new DispatcherEventListener(classLoader);
        OperatorEventDispatcherImpl dispatcher =
                new OperatorEventDispatcherImpl(classLoader, eventListener);
        OperatorID operatorID = new OperatorID();

        OperatorEventGateway gateway = dispatcher.getOperatorEventGateway(operatorID);
        gateway.sendEventToCoordinator(new TestOperatorEvent(0));

        assertThat(eventListener.sentEventsMap).containsOnlyKeys(operatorID);
        assertThat(eventListener.sentEventsMap.get(operatorID))
                .containsExactly(new TestOperatorEvent(0));
    }

    @Test
    public void testReceiveAndForwardOperatorEvent() throws Exception {
        DispatcherEventListener eventListener = new DispatcherEventListener(classLoader);
        OperatorEventDispatcherImpl dispatcher =
                new OperatorEventDispatcherImpl(classLoader, eventListener);
        OperatorID operatorID = new OperatorID();
        dispatcher.registerEventHandler(operatorID, eventListener);

        dispatcher.dispatchEventToHandlers(operatorID, serialize(new TestOperatorEvent(0)));

        assertThat(eventListener.receivedEvents).containsExactly(new TestOperatorEvent(0));
    }

    @Test
    public void testCloseGatewayOnCheckpoint() throws Exception {
        DispatcherEventListener eventListener = new DispatcherEventListener(classLoader);
        OperatorEventDispatcherImpl dispatcher =
                new OperatorEventDispatcherImpl(classLoader, eventListener);
        OperatorID operatorID = new OperatorID();

        OperatorEventGateway gateway = dispatcher.getOperatorEventGateway(operatorID);
        dispatcher.dispatchEventToHandlers(operatorID, serialize(new CloseGatewayEvent(0L, 0)));
        gateway.sendEventToCoordinator(new TestOperatorEvent(0));

        assertThat(eventListener.sentEventsMap).containsOnlyKeys(operatorID);
        assertThat(eventListener.sentEventsMap.get(operatorID))
                .containsExactly(new AcknowledgeCloseGatewayEvent(0L, 0));
    }

    @Test
    public void testReopenGatewayOnCompletedCheckpoint() throws Exception {
        DispatcherEventListener eventListener = new DispatcherEventListener(classLoader);
        OperatorEventDispatcherImpl dispatcher =
                new OperatorEventDispatcherImpl(classLoader, eventListener);
        OperatorID operatorID = new OperatorID();

        OperatorEventGateway gateway = dispatcher.getOperatorEventGateway(operatorID);
        dispatcher.dispatchEventToHandlers(operatorID, serialize(new CloseGatewayEvent(0L, 0)));
        gateway.sendEventToCoordinator(new TestOperatorEvent(0));
        dispatcher.snapshotOperatorEventGatewayState(operatorID, new MockOperatorStateStore());
        dispatcher.notifyOperatorSnapshotStateCompleted(operatorID, 0L, 0);

        assertThat(eventListener.sentEventsMap).containsOnlyKeys(operatorID);
        assertThat(eventListener.sentEventsMap.get(operatorID))
                .containsExactly(
                        new AcknowledgeCloseGatewayEvent(0L, 0),
                        new AcknowledgeCheckpointEvent(0L, 0),
                        new TestOperatorEvent(0));
    }

    @Test
    public void testReloadOperatorEventFromSnapshot() throws Exception {
        OperatorStateStore operatorStateStore = new MockOperatorStateStore();
        DispatcherEventListener eventListener = new DispatcherEventListener(classLoader);
        OperatorEventDispatcherImpl dispatcher =
                new OperatorEventDispatcherImpl(classLoader, eventListener);
        OperatorID operatorID = new OperatorID();

        OperatorEventGateway gateway = dispatcher.getOperatorEventGateway(operatorID);
        dispatcher.dispatchEventToHandlers(operatorID, serialize(new CloseGatewayEvent(0L, 0)));
        gateway.sendEventToCoordinator(new TestOperatorEvent(0));
        dispatcher.snapshotOperatorEventGatewayState(operatorID, operatorStateStore);
        dispatcher.notifyOperatorSnapshotStateCompleted(operatorID, 0L, 0);

        eventListener.sentEventsMap.clear();

        OperatorEventDispatcherImpl reloadedDispatcher =
                new OperatorEventDispatcherImpl(classLoader, eventListener);
        reloadedDispatcher.initializeOperatorEventGatewayState(operatorID, operatorStateStore);

        assertThat(eventListener.sentEventsMap).containsOnlyKeys(operatorID);
        assertThat(eventListener.sentEventsMap.get(operatorID))
                .containsExactly(new TestOperatorEvent(0));
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

    /**
     * A class that monitors all events sent by an {@link OperatorEventDispatcherImpl}, and all
     * events that the {@link OperatorEventDispatcherImpl} has received and forwarded to a specific
     * operator.
     */
    private static class DispatcherEventListener
            implements TaskOperatorEventGateway, OperatorEventHandler {
        private final ClassLoader classLoader;

        private final Map<OperatorID, List<OperatorEvent>> sentEventsMap;

        private final List<OperatorEvent> receivedEvents;

        private DispatcherEventListener(ClassLoader classLoader) {
            this.classLoader = classLoader;
            this.sentEventsMap = new HashMap<>();
            this.receivedEvents = new ArrayList<>();
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

        @Override
        public void handleOperatorEvent(OperatorEvent evt) {
            receivedEvents.add(evt);
        }
    }
}
