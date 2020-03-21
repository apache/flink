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

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.util.SerializableFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * A simple testing implementation of the {@link OperatorCoordinator}.
 */
class TestingOperatorCoordinator implements OperatorCoordinator {

	private final OperatorCoordinator.Context context;

	private final ArrayList<Integer> failedTasks = new ArrayList<>();

	private boolean started;
	private boolean closed;

	public TestingOperatorCoordinator(OperatorCoordinator.Context context) {
		this.context = context;
	}

	// ------------------------------------------------------------------------

	@Override
	public void start() throws Exception {
		started = true;
	}

	@Override
	public void close() {
		closed = true;
	}

	@Override
	public void handleEventFromOperator(int subtask, OperatorEvent event) {}

	@Override
	public void subtaskFailed(int subtask) {
		failedTasks.add(subtask);
	}

	@Override
	public CompletableFuture<byte[]> checkpointCoordinator(long checkpointId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void checkpointComplete(long checkpointId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void resetToCheckpoint(byte[] checkpointData) {
		throw new UnsupportedOperationException();
	}

	// ------------------------------------------------------------------------

	public OperatorCoordinator.Context getContext() {
		return context;
	}

	public boolean isStarted() {
		return started;
	}

	public boolean isClosed() {
		return closed;
	}

	public Collection<Integer> getFailedTasks() {
		return failedTasks;
	}

	// ------------------------------------------------------------------------
	//  The provider for this coordinator implementation
	// ------------------------------------------------------------------------

	/**
	 * A testing stub for an {@link OperatorCoordinator.Provider} that creates a
	 * {@link TestingOperatorCoordinator}.
	 */
	public static final class Provider implements OperatorCoordinator.Provider {

		private static final long serialVersionUID = 1L;

		private final OperatorID operatorId;

		private final SerializableFunction<Context, TestingOperatorCoordinator> factory;

		public Provider(OperatorID operatorId) {
			this(operatorId, TestingOperatorCoordinator::new);
		}

		public Provider(OperatorID operatorId, SerializableFunction<Context, TestingOperatorCoordinator> factory) {
			this.operatorId = operatorId;
			this.factory = factory;
		}

		@Override
		public OperatorID getOperatorId() {
			return operatorId;
		}

		@Override
		public OperatorCoordinator create(OperatorCoordinator.Context context) {
			return factory.apply(context);
		}
	}
}
