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

package org.apache.flink.table.runtime.operators.dynamicfiltering;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestHandler;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.CoordinatorStore;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.RecreateOnResetOperatorCoordinator;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.table.connector.source.DynamicFilteringData;
import org.apache.flink.table.connector.source.DynamicFilteringEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The operator coordinator for {@link DynamicFilteringDataCollectorOperator}. The coordinator
 * collects {@link DynamicFilteringEvent} then redistributes to listening source coordinators.
 */
public class DynamicFilteringDataCollectorOperatorCoordinator
        implements OperatorCoordinator, CoordinationRequestHandler {

    private static final Logger LOG =
            LoggerFactory.getLogger(DynamicFilteringDataCollectorOperatorCoordinator.class);

    private final CoordinatorStore coordinatorStore;
    private final List<String> dynamicFilteringDataListenerIDs;

    private DynamicFilteringData receivedFilteringData;

    public DynamicFilteringDataCollectorOperatorCoordinator(
            Context context, List<String> dynamicFilteringDataListenerIDs) {
        this.coordinatorStore = checkNotNull(context.getCoordinatorStore());
        this.dynamicFilteringDataListenerIDs = checkNotNull(dynamicFilteringDataListenerIDs);
    }

    @Override
    public void start() throws Exception {}

    @Override
    public void close() throws Exception {}

    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event)
            throws Exception {
        DynamicFilteringData currentData =
                ((DynamicFilteringEvent) ((SourceEventWrapper) event).getSourceEvent()).getData();
        if (receivedFilteringData == null) {
            receivedFilteringData = currentData;
        } else {
            // Since there might be speculative execution or failover, we may receive multiple
            // notifications, and we can't tell for sure which one is valid for further processing.
            if (DynamicFilteringData.isEqual(receivedFilteringData, currentData)) {
                // If the notifications contain exactly the same data, everything is alright, and
                // we don't need to send the event again.
                return;
            } else {
                // In case the mismatching of the source filtering result and the dim data, which
                // may leads to incorrect result, trigger global failover for fully recomputing.
                throw new IllegalStateException(
                        "DynamicFilteringData is recomputed but not equal. "
                                + "Triggering global failover in case the result is incorrect. "
                                + " It's recommended to re-run the job with dynamic filtering disabled.");
            }
        }

        for (String listenerID : dynamicFilteringDataListenerIDs) {
            // Push event to listening source coordinators.
            OperatorCoordinator listener = (OperatorCoordinator) coordinatorStore.get(listenerID);
            if (listener == null) {
                throw new IllegalStateException(
                        "Dynamic filtering data listener is missing: " + listenerID);
            } else {
                LOG.info(
                        "Distributing event {} to source coordinator with ID {}",
                        event,
                        listenerID);
                // Subtask index and attempt number is not necessary for handling
                // DynamicFilteringEvent.
                listener.handleEventFromOperator(0, 0, event);
            }
        }
    }

    @Override
    public CompletableFuture<CoordinationResponse> handleCoordinationRequest(
            CoordinationRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {}

    @Override
    public void executionAttemptFailed(
            int subtask, int attemptNumber, @Nullable Throwable reason) {}

    @Override
    public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {}

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result)
            throws Exception {}

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    @Override
    public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData)
            throws Exception {}

    /** Provider for {@link DynamicFilteringDataCollectorOperatorCoordinator}. */
    public static class Provider extends RecreateOnResetOperatorCoordinator.Provider {
        private final List<String> dynamicFilteringDataListenerIDs;

        public Provider(OperatorID operatorID, List<String> dynamicFilteringDataListenerIDs) {
            super(operatorID);
            this.dynamicFilteringDataListenerIDs = checkNotNull(dynamicFilteringDataListenerIDs);
        }

        @Override
        protected OperatorCoordinator getCoordinator(Context context) throws Exception {
            return new DynamicFilteringDataCollectorOperatorCoordinator(
                    context, dynamicFilteringDataListenerIDs);
        }
    }
}
