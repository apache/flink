/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.Acknowledge;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

/**
 * A class that will recreate a new {@link OperatorCoordinator} instance when
 * reset to checkpoint.
 */
public class RecreateOnResetOperatorCoordinator implements OperatorCoordinator {

	private final Provider provider;
	private QuiesceableContext quiesceableContext;
	private OperatorCoordinator coordinator;

	private boolean started;

	private RecreateOnResetOperatorCoordinator(
			QuiesceableContext context,
			Provider provider) {
		this.quiesceableContext = context;
		this.provider = provider;
		this.coordinator = provider.getCoordinator(context);
		this.started = false;
	}

	@Override
	public void start() throws Exception {
		coordinator.start();
		started = true;
	}

	@Override
	public void close() throws Exception {
		coordinator.close();
	}

	@Override
	public void handleEventFromOperator(int subtask, OperatorEvent event) throws Exception {
		coordinator.handleEventFromOperator(subtask, event);
	}

	@Override
	public void subtaskFailed(int subtask, @Nullable Throwable reason) {
		coordinator.subtaskFailed(subtask, reason);
	}

	@Override
	public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) throws Exception {
		coordinator.checkpointCoordinator(checkpointId, resultFuture);
	}

	@Override
	public void checkpointComplete(long checkpointId) {
		coordinator.checkpointComplete(checkpointId);
	}

	@Override
	public void resetToCheckpoint(byte[] checkpointData) throws Exception {
		// Quiesce the context so the coordinator cannot interact with the job master anymore.
		quiesceableContext.quiesce();
		// Close the coordinator.
		coordinator.close();
		// Create a new coordinator and reset to the checkpoint.
		quiesceableContext = new QuiesceableContext(quiesceableContext.getContext());
		coordinator = provider.getCoordinator(quiesceableContext);
		coordinator.resetToCheckpoint(checkpointData);
		// Start the new coordinator if this coordinator has been started before reset to the checkpoint.
		if (started) {
			coordinator.start();
		}
	}

	// ---------------------

	@VisibleForTesting
	public OperatorCoordinator getInternalCoordinator() {
		return coordinator;
	}

	@VisibleForTesting
	QuiesceableContext getQuiesceableContext() {
		return quiesceableContext;
	}

	// ---------------------

	public static abstract class Provider implements OperatorCoordinator.Provider {
		private static final long serialVersionUID = 3002837631612629071L;
		private final OperatorID operatorID;

		public Provider(OperatorID operatorID) {
			this.operatorID = operatorID;
		}

		@Override
		public OperatorID getOperatorId() {
			return operatorID;
		}

		@Override
		public OperatorCoordinator create(Context context) {
			QuiesceableContext quiesceableContext = new QuiesceableContext(context);
			return new RecreateOnResetOperatorCoordinator(quiesceableContext, this);
		}

		protected abstract OperatorCoordinator getCoordinator(OperatorCoordinator.Context context);
	}

	// ----------------------

	/**
	 * A wrapper class around the operator coordinator context to allow quiescence.
	 * When a new operator coordinator is created, we need to quiesce the old
	 * operator coordinator to prevent it from making any further impact to the
	 * job master. This is done by quiesce the operator coordinator context.
	 * After the quiescence, the "reading" methods will still work, but the
	 * "writing" methods will become a no-op or fail immediately.
	 */
	@VisibleForTesting
	static class QuiesceableContext implements OperatorCoordinator.Context {
		private final OperatorCoordinator.Context context;
		private volatile boolean quiesced;

		QuiesceableContext(OperatorCoordinator.Context context) {
			this.context = context;
			quiesced = false;
		}

		@Override
		public OperatorID getOperatorId() {
			return context.getOperatorId();
		}

		@Override
		public synchronized CompletableFuture<Acknowledge> sendEvent(
				OperatorEvent evt,
				int targetSubtask) throws TaskNotRunningException {
			// Do not enter the sending procedure if the context has been quiesced.
			if (quiesced) {
				return CompletableFuture.completedFuture(Acknowledge.get());
			}
			return context.sendEvent(evt, targetSubtask);
		}

		@Override
		public synchronized void failJob(Throwable cause) {
			if (quiesced) {
				return;
			}
			context.failJob(cause);
		}

		@Override
		public int currentParallelism() {
			return context.currentParallelism();
		}

		@VisibleForTesting
		synchronized void quiesce() {
			quiesced = true;
		}

		@VisibleForTesting
		boolean isQuiesced() {
			return quiesced;
		}

		private OperatorCoordinator.Context getContext() {
			return context;
		}
	}
}
