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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.AcknowledgeCloseGatewayEvent;
import org.apache.flink.runtime.operators.coordination.CloseGatewayEvent;
import org.apache.flink.runtime.operators.coordination.OpenGatewayEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.SerializedValue;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of the {@link OperatorEventDispatcher}.
 *
 * <p>This class is intended for single threaded use from the stream task mailbox.
 */
@Internal
public final class OperatorEventDispatcherImpl implements OperatorEventDispatcher {

    private final Map<OperatorID, OperatorEventHandler> handlers;

    private final ClassLoader classLoader;

    private final TaskOperatorEventGateway toCoordinator;

    private final Map<OperatorID, OperatorEventGatewayImpl> gatewayMap;

    public OperatorEventDispatcherImpl(
            ClassLoader classLoader, TaskOperatorEventGateway toCoordinator) {
        this.classLoader = checkNotNull(classLoader);
        this.toCoordinator = checkNotNull(toCoordinator);
        this.handlers = new HashMap<>();
        this.gatewayMap = new HashMap<>();
    }

    void dispatchEventToHandlers(
            OperatorID operatorID, SerializedValue<OperatorEvent> serializedEvent)
            throws FlinkException {
        final OperatorEvent evt;
        try {
            evt = serializedEvent.deserializeValue(classLoader);
        } catch (IOException | ClassNotFoundException e) {
            throw new FlinkException("Could not deserialize operator event", e);
        }

        if (evt instanceof CloseGatewayEvent) {
            OperatorEventGatewayImpl gateway = getOperatorEventGateway(operatorID);
            gateway.closeGateway(((CloseGatewayEvent) evt).getCheckpointID());
            gateway.sendEventToCoordinator(
                    new AcknowledgeCloseGatewayEvent((CloseGatewayEvent) evt), false);
            return;
        } else if (evt instanceof OpenGatewayEvent) {
            OperatorEventGatewayImpl gateway = getOperatorEventGateway(operatorID);
            gateway.openGateway(((OpenGatewayEvent) evt).getCheckpointID());
            return;
        }

        final OperatorEventHandler handler = handlers.get(operatorID);
        if (handler != null) {
            handler.handleOperatorEvent(evt);
        } else {
            throw new FlinkException("Operator not registered for operator events");
        }
    }

    void initializeOperatorEventGatewayIfExists(StreamOperator<?> operator) throws Exception {
        if (gatewayMap.containsKey(operator.getOperatorID())) {
            getOperatorEventGateway(operator.getOperatorID())
                    .initializeState(getOperatorStateBackend(operator));
        }
    }

    void snapshotOperatorEventGatewayIfExists(StreamOperator<?> operator) throws Exception {
        if (gatewayMap.containsKey(operator.getOperatorID())) {
            getOperatorEventGateway(operator.getOperatorID())
                    .snapshotState(getOperatorStateBackend(operator));
        }
    }

    void notifyCheckpointAbortedIfExists(StreamOperator<?> operator, long checkpointId) {
        if (gatewayMap.containsKey(operator.getOperatorID())) {
            OperatorEventGatewayImpl gateway = getOperatorEventGateway(operator.getOperatorID());
            gateway.openGateway(checkpointId);
        }
    }

    void notifyOperatorSnapshotCreatedIfExists(StreamOperator<?> operator, long checkpointId) {
        if (gatewayMap.containsKey(operator.getOperatorID())) {
            OperatorEventGatewayImpl gateway = getOperatorEventGateway(operator.getOperatorID());
            gateway.sendEventToCoordinator(
                    new OpenGatewayEvent(checkpointId, getSubtaskIndex(operator)), false);
            gateway.openGateway(checkpointId);
        }
    }

    @Override
    public void registerEventHandler(OperatorID operator, OperatorEventHandler handler) {
        final OperatorEventHandler prior = handlers.putIfAbsent(operator, handler);
        if (prior != null) {
            throw new IllegalStateException("already a handler registered for this operatorId");
        }
    }

    @Override
    public OperatorEventGatewayImpl getOperatorEventGateway(OperatorID operatorId) {
        return gatewayMap.computeIfAbsent(
                operatorId, key -> new OperatorEventGatewayImpl(toCoordinator, key));
    }

    private OperatorStateBackend getOperatorStateBackend(StreamOperator<?> operator) {
        if (operator instanceof AbstractStreamOperator) {
            return ((AbstractStreamOperator<?>) operator).getOperatorStateBackend();
        } else if (operator instanceof AbstractStreamOperatorV2) {
            return ((AbstractStreamOperatorV2<?>) operator).getOperatorStateBackend();
        } else {
            throw new IllegalStateException(
                    "Operator "
                            + operator
                            + " should extend AbstractStreamOperator or AbstractStreamOperatorV2"
                            + " to provide OperatorStateBackend for OperatorEventGateway.");
        }
    }

    private int getSubtaskIndex(StreamOperator<?> operator) {
        if (operator instanceof AbstractStreamOperator) {
            return ((AbstractStreamOperator<?>) operator)
                    .getRuntimeContext()
                    .getIndexOfThisSubtask();
        } else if (operator instanceof AbstractStreamOperatorV2) {
            return ((AbstractStreamOperatorV2<?>) operator)
                    .getRuntimeContext()
                    .getIndexOfThisSubtask();
        } else {
            throw new IllegalStateException(
                    "Operator "
                            + operator
                            + " should extend AbstractStreamOperator or AbstractStreamOperatorV2"
                            + " to provide a RuntimeContext.");
        }
    }

    // ------------------------------------------------------------------------

    private static final class OperatorEventGatewayImpl implements OperatorEventGateway {

        private static final String BLOCKED_EVENTS_STATE_KEY = "blockedOperatorEvents";

        private final TaskOperatorEventGateway toCoordinator;

        private final OperatorID operatorId;

        private final ListStateDescriptor<SerializedOperatorEvent> descriptor;

        private final TreeMap<Long, List<SerializedOperatorEvent>> blockedEventsMap;

        private OperatorEventGatewayImpl(
                TaskOperatorEventGateway toCoordinator, OperatorID operatorId) {
            this.toCoordinator = toCoordinator;
            this.descriptor =
                    new ListStateDescriptor<>(
                            BLOCKED_EVENTS_STATE_KEY,
                            TypeInformation.of(SerializedOperatorEvent.class));
            this.operatorId = operatorId;
            this.blockedEventsMap = new TreeMap<>();
        }

        @Override
        public void sendEventToCoordinator(OperatorEvent event) {
            sendEventToCoordinator(event, true);
        }

        private void sendEventToCoordinator(OperatorEvent event, boolean canBeBlocked) {
            final SerializedOperatorEvent serializedEvent;
            try {
                serializedEvent = new SerializedOperatorEvent(event);
            } catch (IOException e) {
                // this is not a recoverable situation, so we wrap this in an
                // unchecked exception and let it bubble up
                throw new FlinkRuntimeException("Cannot serialize operator event", e);
            }

            if (canBeBlocked && !blockedEventsMap.isEmpty()) {
                blockedEventsMap.lastEntry().getValue().add(serializedEvent);
            } else {
                toCoordinator.sendOperatorEventToCoordinator(operatorId, serializedEvent);
            }
        }

        private void closeGateway(long checkpointId) {
            blockedEventsMap.putIfAbsent(checkpointId, new LinkedList<>());
        }

        private void openGateway(long checkpointId) {
            if (blockedEventsMap.containsKey(checkpointId)) {
                if (blockedEventsMap.firstKey() == checkpointId) {
                    for (SerializedValue<OperatorEvent> blockedEvent :
                            blockedEventsMap.firstEntry().getValue()) {
                        toCoordinator.sendOperatorEventToCoordinator(operatorId, blockedEvent);
                    }
                } else {
                    blockedEventsMap
                            .floorEntry(checkpointId - 1)
                            .getValue()
                            .addAll(blockedEventsMap.get(checkpointId));
                }
                blockedEventsMap.remove(checkpointId);
            }
        }

        private void initializeState(OperatorStateStore operatorStateStore) throws Exception {
            ListState<SerializedOperatorEvent> listState =
                    operatorStateStore.getListState(descriptor);
            for (SerializedValue<OperatorEvent> blockedEvent : listState.get()) {
                toCoordinator.sendOperatorEventToCoordinator(operatorId, blockedEvent);
            }
            listState.clear();
        }

        private void snapshotState(OperatorStateStore operatorStateStore) throws Exception {
            ListState<SerializedOperatorEvent> listState =
                    operatorStateStore.getListState(descriptor);
            listState.clear();
            for (List<SerializedOperatorEvent> events : blockedEventsMap.values()) {
                listState.addAll(events);
            }
        }

        /**
         * A wrapper class of serialized operator event to resolve Java's limit on the usage of
         * classes with generics.
         */
        private static class SerializedOperatorEvent extends SerializedValue<OperatorEvent> {
            public SerializedOperatorEvent(OperatorEvent value) throws IOException {
                super(value);
            }
        }
    }
}
