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

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

/**
 * A class that will recreate a new {@link OperatorCoordinator} instance when
 * reset to checkpoint.
 */
public class RecreateOnResetOperatorCoordinator implements OperatorCoordinator {

	private final OperatorCoordinator.Context context;
	private final OperatorCoordinator.Provider provider;
	private OperatorCoordinator coordinator;

	public RecreateOnResetOperatorCoordinator(
			OperatorCoordinator.Context context,
			OperatorCoordinator.Provider provider,
			OperatorCoordinator coordinator) {
		this.context = context;
		this.provider = provider;
		this.coordinator = coordinator;
	}

	@Override
	public void start() throws Exception {
		coordinator.start();
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
		coordinator.close();
		coordinator = createNewCoordinator();
		coordinator.resetToCheckpoint(checkpointData);
		coordinator.start();
	}

	// ---------------------

	@VisibleForTesting
	public OperatorCoordinator getInternalCoordinator() {
		return coordinator;
	}

	// ---------------------

	private OperatorCoordinator createNewCoordinator() {
		return maybeUnwrap(provider.create(context));
	}

	private static OperatorCoordinator maybeUnwrap(OperatorCoordinator coordinator) {
		if (coordinator instanceof RecreateOnResetOperatorCoordinator) {
			return maybeUnwrap(((RecreateOnResetOperatorCoordinator) coordinator).coordinator);
		} else {
			return coordinator;
		}
	}
}
