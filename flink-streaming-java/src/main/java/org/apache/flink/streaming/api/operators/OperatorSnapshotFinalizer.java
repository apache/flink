/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.util.FutureUtil;

import java.util.concurrent.ExecutionException;

/**
 * TODO write comment.
 */
public class OperatorSnapshotFinalizer {

	private final OperatorSubtaskState jobManagerOwnedState;
	private final OperatorSubtaskState taskLocalState;

	public OperatorSnapshotFinalizer(
		OperatorSnapshotFutures snapshotFutures) throws ExecutionException, InterruptedException {

		SnapshotResult<KeyedStateHandle> keyedManaged =
			FutureUtil.runIfNotDoneAndGet(snapshotFutures.getKeyedStateManagedFuture());

		SnapshotResult<KeyedStateHandle> keyedRaw =
			FutureUtil.runIfNotDoneAndGet(snapshotFutures.getKeyedStateRawFuture());

		SnapshotResult<OperatorStateHandle> operatorManaged =
			FutureUtil.runIfNotDoneAndGet(snapshotFutures.getOperatorStateManagedFuture());

		SnapshotResult<OperatorStateHandle> operatorRaw =
			FutureUtil.runIfNotDoneAndGet(snapshotFutures.getOperatorStateRawFuture());

		jobManagerOwnedState = new OperatorSubtaskState(
			extractJobManagerOwned(operatorManaged),
			extractJobManagerOwned(operatorRaw),
			extractJobManagerOwned(keyedManaged),
			extractJobManagerOwned(keyedRaw)
		);

		taskLocalState = new OperatorSubtaskState(
			extractTaskLocal(operatorManaged),
			extractTaskLocal(operatorRaw),
			extractTaskLocal(keyedManaged),
			extractTaskLocal(keyedRaw)
		);
	}

	public OperatorSubtaskState getTaskLocalState() {
		return taskLocalState;
	}

	public OperatorSubtaskState getJobManagerOwnedState() {
		return jobManagerOwnedState;
	}

	private <T extends StateObject> T extractJobManagerOwned(SnapshotResult<T> snapshotResult) {
		return snapshotResult != null ? snapshotResult.getJobManagerOwnedSnapshot() : null;
	}

	private <T extends StateObject> T extractTaskLocal(SnapshotResult<T> snapshotResult) {
		return snapshotResult != null ? snapshotResult.getTaskLocalSnapshot() : null;
	}
}
