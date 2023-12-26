/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.collect;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestHandler;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * {@link OperatorCoordinator} for {@link CollectSinkFunction}.
 *
 * <p>This coordinator only forwards requests and responses from clients and sinks and it does not
 * store any results in itself.
 */
public class CollectSinkOperatorCoordinator
        implements OperatorCoordinator, CoordinationRequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(CollectSinkOperatorCoordinator.class);

    private final int socketTimeout;

    private InetSocketAddress address;

    private SocketConnection socketConnection;

    private final Set<CompletableFuture<CoordinationResponse>> ongoingRequests =
            ConcurrentHashMap.newKeySet();

    private ExecutorService executorService;

    @VisibleForTesting
    CollectSinkOperatorCoordinator() {
        this(0);
    }

    public CollectSinkOperatorCoordinator(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    @Override
    public void start() throws Exception {
        this.executorService =
                Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory(
                                "collect-sink-operator-coordinator-executor-thread-pool"));
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing the CollectSinkOperatorCoordinator.");
        this.executorService.shutdownNow();

        // cancelling all ongoing requests explicitly
        ongoingRequests.forEach(ft -> ft.cancel(true));
        ongoingRequests.clear();

        closeConnection();
    }

    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event)
            throws Exception {
        Preconditions.checkArgument(
                event instanceof CollectSinkAddressEvent,
                "Operator event must be a CollectSinkAddressEvent");
        address = ((CollectSinkAddressEvent) event).getAddress();
        LOG.info("Received sink socket server address: " + address);
    }

    @Override
    public CompletableFuture<CoordinationResponse> handleCoordinationRequest(
            CoordinationRequest request) {
        Preconditions.checkArgument(
                request instanceof CollectCoordinationRequest,
                "Coordination request must be a CollectCoordinationRequest");

        CollectCoordinationRequest collectRequest = (CollectCoordinationRequest) request;
        if (address == null) {
            return CompletableFuture.completedFuture(createEmptyResponse(collectRequest));
        }

        final CompletableFuture<CoordinationResponse> responseFuture =
                FutureUtils.supplyAsync(
                        () -> handleRequestImpl(collectRequest, address), executorService);

        ongoingRequests.add(responseFuture);
        return responseFuture.handle(
                (response, error) -> {
                    ongoingRequests.remove(responseFuture);

                    if (response != null) {
                        return response;
                    }

                    // cancelling the future implies that the error handling happens somewhere else
                    if (!ExceptionUtils.findThrowable(error, CancellationException.class)
                            .isPresent()) {
                        // Request failed: Close current connection and send back empty results
                        // we catch every exception here because the Socket might suddenly become
                        // null. We don't want the coordinator to fail if the sink fails.
                        if (LOG.isDebugEnabled()) {
                            LOG.warn(
                                    "Collect sink coordinator encountered an unexpected error.",
                                    error);
                        } else {
                            LOG.warn(
                                    "Collect sink coordinator encounters a {}: {}",
                                    error.getClass().getSimpleName(),
                                    error.getMessage());
                        }

                        closeConnection();
                    }

                    return createEmptyResponse(collectRequest);
                });
    }

    private CoordinationResponse handleRequestImpl(
            CollectCoordinationRequest request, InetSocketAddress sinkAddress) throws IOException {
        if (sinkAddress == null) {
            throw new NullPointerException("No sinkAddress available.");
        }

        if (socketConnection == null) {
            socketConnection = SocketConnection.create(socketTimeout, sinkAddress);
            LOG.info("Sink connection established");
        }

        // send version and offset to sink server
        LOG.debug("Forwarding request to sink socket server");
        request.serialize(socketConnection.getDataOutputView());

        // fetch back serialized results
        LOG.debug("Fetching serialized result from sink socket server");
        return new CollectCoordinationResponse(socketConnection.getDataInputView());
    }

    private CollectCoordinationResponse createEmptyResponse(CollectCoordinationRequest request) {
        return new CollectCoordinationResponse(
                request.getVersion(),
                // this lastCheckpointedOffset is OK
                // because client will only expose results to the users when the
                // checkpointed offset increases
                -1,
                Collections.emptyList());
    }

    private void closeConnection() {
        if (socketConnection != null) {
            try {
                socketConnection.close();
            } catch (Exception e) {
                LOG.warn("Failed to close sink socket server connection", e);
            }
            socketConnection = null;
        }
    }

    @Override
    public void executionAttemptFailed(int subtask, int attemptNumber, @Nullable Throwable reason) {
        // attempt failed, the socket server does not exist anymore
        address = null;
    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        // nothing to do here, connections are re-created lazily
    }

    @Override
    public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {
        // nothing to do here, connections are re-created lazily
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result)
            throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(address);

        result.complete(baos.toByteArray());
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    @Override
    public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData)
            throws Exception {
        if (checkpointData == null) {
            // restore before any checkpoint completed
            LOG.info("Any ongoing requests are cancelled due to a coordinator reset.");
            cancelOngoingRequests();

            closeConnection();
        } else {
            ByteArrayInputStream bais = new ByteArrayInputStream(checkpointData);
            ObjectInputStream ois = new ObjectInputStream(bais);
            address = (InetSocketAddress) ois.readObject();
        }
    }

    private void cancelOngoingRequests() {
        ongoingRequests.forEach(ft -> ft.cancel(true));
        ongoingRequests.clear();
    }

    /** Provider for {@link CollectSinkOperatorCoordinator}. */
    public static class Provider implements OperatorCoordinator.Provider {

        private final OperatorID operatorId;
        private final int socketTimeout;

        public Provider(OperatorID operatorId, int socketTimeout) {
            this.operatorId = operatorId;
            this.socketTimeout = socketTimeout;
        }

        @Override
        public OperatorID getOperatorId() {
            return operatorId;
        }

        @Override
        public OperatorCoordinator create(Context context) {
            // we do not send operator event so we don't need a context
            return new CollectSinkOperatorCoordinator(socketTimeout);
        }
    }
}
