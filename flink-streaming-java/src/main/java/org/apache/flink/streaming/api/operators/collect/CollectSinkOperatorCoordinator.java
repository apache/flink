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

import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestHandler;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
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
    private Socket socket;
    private DataInputViewStreamWrapper inStream;
    private DataOutputViewStreamWrapper outStream;

    private ExecutorService executorService;

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
        closeConnection();
        this.executorService.shutdown();
    }

    @Override
    public void handleEventFromOperator(int subtask, OperatorEvent event) throws Exception {
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
        CompletableFuture<CoordinationResponse> responseFuture = new CompletableFuture<>();

        if (address == null) {
            completeWithEmptyResponse(collectRequest, responseFuture);
            return responseFuture;
        }

        executorService.submit(() -> handleRequestImpl(collectRequest, responseFuture, address));
        return responseFuture;
    }

    private void handleRequestImpl(
            CollectCoordinationRequest request,
            CompletableFuture<CoordinationResponse> responseFuture,
            InetSocketAddress sinkAddress) {
        if (sinkAddress == null) {
            closeConnection();
            completeWithEmptyResponse(request, responseFuture);
            return;
        }

        try {
            if (socket == null) {
                socket = new Socket();
                socket.setSoTimeout(socketTimeout);
                socket.setKeepAlive(true);
                socket.setTcpNoDelay(true);

                socket.connect(sinkAddress);
                inStream = new DataInputViewStreamWrapper(socket.getInputStream());
                outStream = new DataOutputViewStreamWrapper(socket.getOutputStream());
                LOG.info("Sink connection established");
            }

            // send version and offset to sink server
            if (LOG.isDebugEnabled()) {
                LOG.debug("Forwarding request to sink socket server");
            }
            request.serialize(outStream);

            // fetch back serialized results
            if (LOG.isDebugEnabled()) {
                LOG.debug("Fetching serialized result from sink socket server");
            }
            responseFuture.complete(new CollectCoordinationResponse(inStream));
        } catch (Exception e) {
            // request failed, close current connection and send back empty results
            // we catch every exception here because socket might suddenly becomes null if the sink
            // fails
            // and we do not want the coordinator to fail
            if (LOG.isDebugEnabled()) {
                // this is normal when sink restarts or job ends, so we print a debug log
                LOG.debug("Collect sink coordinator encounters an exception", e);
            }
            closeConnection();
            completeWithEmptyResponse(request, responseFuture);
        }
    }

    private void completeWithEmptyResponse(
            CollectCoordinationRequest request, CompletableFuture<CoordinationResponse> future) {
        future.complete(
                new CollectCoordinationResponse(
                        request.getVersion(),
                        // this lastCheckpointedOffset is OK
                        // because client will only expose results to the users when the
                        // checkpointed offset increases
                        -1,
                        Collections.emptyList()));
    }

    private void closeConnection() {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
                LOG.warn("Failed to close sink socket server connection", e);
            }
        }
        socket = null;
    }

    @Override
    public void subtaskFailed(int subtask, @Nullable Throwable reason) {
        // subtask failed, the socket server does not exist anymore
        address = null;
    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        // nothing to do here, connections are re-created lazily
    }

    @Override
    public void subtaskReady(int subtask, SubtaskGateway gateway) {
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
            closeConnection();
        } else {
            ByteArrayInputStream bais = new ByteArrayInputStream(checkpointData);
            ObjectInputStream ois = new ObjectInputStream(bais);
            address = (InetSocketAddress) ois.readObject();
        }
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
