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

import org.apache.flink.api.common.typeutils.base.LongSerializer;
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
 * <p>This coordinator only forwards requests and responses from clients and sinks
 * and it does not store any results in itself.
 */
public class CollectSinkOperatorCoordinator implements OperatorCoordinator, CoordinationRequestHandler {

	private static final Logger LOG = LoggerFactory.getLogger(CollectSinkOperatorCoordinator.class);

	private InetSocketAddress address;
	private Socket socket;
	private DataInputViewStreamWrapper inStream;
	private DataOutputViewStreamWrapper outStream;

	private ExecutorService executorService;

	@Override
	public void start() throws Exception {
		this.executorService =
			Executors.newSingleThreadExecutor(
				new ExecutorThreadFactory(
					"collect-sink-operator-coordinator-executor-thread-pool"));
	}

	@Override
	public void close() throws Exception {
		this.executorService.shutdown();
		closeCurrentConnection();
	}

	@Override
	public void handleEventFromOperator(int subtask, OperatorEvent event) throws Exception {
		Preconditions.checkArgument(
			event instanceof CollectSinkAddressEvent, "Operator event must be a CollectSinkAddressEvent");
		address = ((CollectSinkAddressEvent) event).getAddress();
		LOG.info("Received sink socket server address: " + address);

		// this event is sent when the sink function starts, so we remove the old socket if it is present
		closeCurrentConnection();
	}

	@Override
	public CompletableFuture<CoordinationResponse> handleCoordinationRequest(CoordinationRequest request) {
		Preconditions.checkArgument(
			request instanceof CollectCoordinationRequest,
			"Coordination request must be a CollectCoordinationRequest");

		CompletableFuture<CoordinationResponse> responseFuture = new CompletableFuture<>();
		executorService.submit(() -> handleRequestImpl((CollectCoordinationRequest) request, responseFuture));
		return responseFuture;
	}

	private void handleRequestImpl(
			CollectCoordinationRequest request,
			CompletableFuture<CoordinationResponse> responseFuture) {
		// we back up the address object here to avoid concurrent modifying from `handleEventFromOperator`.
		// it's ok if this address becomes invalid in the following code,
		// if this happens, the coordinator will just return an empty result
		InetSocketAddress address = this.address;

		if (address == null) {
			completeWithEmptyResponse(request, responseFuture);
			return;
		}

		try {
			if (socket == null) {
				socket = new Socket(address.getAddress(), address.getPort());
				socket.setKeepAlive(true);
				socket.setTcpNoDelay(true);
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
		} catch (IOException e) {
			// request failed, close current connection and send back empty results
			// we catch every exception here because socket might suddenly becomes null if the sink fails
			// and we do not want the coordinator to fail
			closeCurrentConnection();
			completeWithEmptyResponse(request, responseFuture);
		}
	}

	private void completeWithEmptyResponse(
			CollectCoordinationRequest request,
			CompletableFuture<CoordinationResponse> future) {
		try {
			future.complete(new CollectCoordinationResponse<>(
				request.getVersion(),
				// this lastCheckpointedOffset is OK
				// because client will only expose results to the users when the checkpointed offset increases
				0,
				Collections.emptyList(),
				// just a random serializer, we're serializing no results
				LongSerializer.INSTANCE));
		} catch (IOException e) {
			// this is impossible as we're serializing no results
			// but even if this happens, client will come with the same version and offset again
			future.completeExceptionally(e);
		}
	}

	private void closeCurrentConnection() {
		if (socket != null) {
			try {
				socket.close();
			} catch (IOException e) {
				LOG.warn("Failed to close sink socket server connection", e);
			}
			socket = null;
		}
	}

	@Override
	public void subtaskFailed(int subtask) {
		// subtask failed, the socket server does not exist anymore
		address = null;
		closeCurrentConnection();
	}

	@Override
	public CompletableFuture<byte[]> checkpointCoordinator(long checkpointId) throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(address);
		return CompletableFuture.completedFuture(baos.toByteArray());
	}

	@Override
	public void checkpointComplete(long checkpointId) {
	}

	@Override
	public void resetToCheckpoint(byte[] checkpointData) throws Exception {
		ByteArrayInputStream bais = new ByteArrayInputStream(checkpointData);
		ObjectInputStream ois = new ObjectInputStream(bais);
		address = (InetSocketAddress) ois.readObject();
	}

	/**
	 * Provider for {@link CollectSinkOperatorCoordinator}.
	 */
	public static class Provider implements OperatorCoordinator.Provider {

		private final OperatorID operatorId;

		public Provider(OperatorID operatorId) {
			this.operatorId = operatorId;
		}

		@Override
		public OperatorID getOperatorId() {
			return operatorId;
		}

		@Override
		public OperatorCoordinator create(Context context) {
			// we do not send operator event so we don't need a context
			return new CollectSinkOperatorCoordinator();
		}
	}
}
