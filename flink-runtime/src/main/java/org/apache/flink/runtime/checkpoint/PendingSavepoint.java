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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointStore;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointV0;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import scala.concurrent.Future;
import scala.concurrent.Promise;

import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A pending savepoint is like a pending checkpoint, but it additionally performs some
 * actions upon completion, like notifying the triggerer.
 */
public class PendingSavepoint extends PendingCheckpoint {

	private static final Logger LOG = CheckpointCoordinator.LOG;

	private final SavepointStore store;

	/** The promise to fulfill once the savepoint is complete */
	private final Promise<String> onCompletionPromise;
	
	// --------------------------------------------------------------------------------------------

	public PendingSavepoint(
			JobID jobId,
			long checkpointId,
			long checkpointTimestamp,
			Map<ExecutionAttemptID, ExecutionVertex> verticesToConfirm,
			ClassLoader userCodeClassLoader,
			SavepointStore store)
	{
		super(jobId, checkpointId, checkpointTimestamp, verticesToConfirm, userCodeClassLoader, false);

		this.store = checkNotNull(store);
		this.onCompletionPromise = new scala.concurrent.impl.Promise.DefaultPromise<>();
	}

	// --------------------------------------------------------------------------------------------
	//  Savepoint completion
	// --------------------------------------------------------------------------------------------

	public Future<String> getCompletionFuture() {
		return onCompletionPromise.future();
	}
	
	@Override
	public CompletedCheckpoint finalizeCheckpoint() throws Exception {
		// finalize checkpoint (this also disposes this pending checkpoint)
		CompletedCheckpoint completedCheckpoint = super.finalizeCheckpoint();

		// now store the checkpoint externally as a savepoint
		try {
			Savepoint savepoint = new SavepointV0(
					completedCheckpoint.getCheckpointID(),
					completedCheckpoint.getTaskStates().values());
			
			String path = store.storeSavepoint(savepoint);
			onCompletionPromise.success(path);
		}
		catch (Throwable t) {
			LOG.warn("Failed to store savepoint.", t);
			onCompletionPromise.failure(t);

			ExceptionUtils.rethrow(t, "Failed to store savepoint.");
		}

		return completedCheckpoint;
	}

	// ------------------------------------------------------------------------
	//  Cancellation / Disposal
	// ------------------------------------------------------------------------

	@Override
	public boolean canBeSubsumed() {
		return false;
	}

	@Override
	public void abortSubsumed() throws Exception {
		try {
			Exception e = new Exception("Bug: Savepoints must never be subsumed");
			onCompletionPromise.failure(e);
			throw e;
		}
		finally {
			dispose(true);
		}
	}

	@Override
	public void abortExpired() throws Exception {
		try {
			LOG.info("Savepoint with checkpoint ID " + getCheckpointId() + " expired before completing.");
			onCompletionPromise.failure(new Exception("Savepoint expired before completing"));
		}
		finally {
			dispose(true);
		}
	}

	@Override
	public void abortDeclined() throws Exception {
		try {
			LOG.info("Savepoint with checkpoint ID " + getCheckpointId() + " was declined (tasks not ready).");
			onCompletionPromise.failure(new Exception("Savepoint was declined (tasks not ready)"));
		}
		finally {
			dispose(true);
		}
	}

	@Override
	public void abortError(Throwable cause) throws Exception {
		try {
			LOG.info("Savepoint with checkpoint ID " + getCheckpointId() + " failed due to an error", cause);
			onCompletionPromise.failure(
					new Exception("Savepoint could not be completed: " + cause.getMessage(), cause));
		}
		finally {
			dispose(true);
		}
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public String toString() {
		return String.format("Pending Savepoint %d @ %d - confirmed=%d, pending=%d",
				getCheckpointId(), getCheckpointTimestamp(),
				getNumberOfAcknowledgedTasks(), getNumberOfNonAcknowledgedTasks());
	}
}
