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
import org.apache.flink.runtime.messages.Acknowledge;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

/**
 * A coordinator for runtime operators. The OperatorCoordinator runs on the master, associated with
 * the job vertex of the operator. It communicated with operators via sending operator events.
 *
 * <p>Operator coordinators are for example source and sink coordinators that discover and assign
 * work, or aggregate and commit metadata.
 */
public interface OperatorCoordinator extends AutoCloseable {

	/**
	 * Starts the coordinator. This method is called once at the beginning, before any other methods.
	 *
	 * @throws Exception Any exception thrown from this method causes a full job failure.
	 */
	void start() throws Exception;

	/**
	 * This method is called when the coordinator is disposed. This method should release currently
	 * held resources. Exceptions in this method do not cause the job to fail.
	 */
	@Override
	void close() throws Exception;

	// ------------------------------------------------------------------------

	/**
	 * Hands an OperatorEvent from a task (on the Task Manager) to this coordinator.
	 *
	 * @throws Exception Any exception thrown by this method results in a full job failure and recovery.
	 */
	void handleEventFromOperator(int subtask, OperatorEvent event) throws Exception;

	// ------------------------------------------------------------------------

	/**
	 * Called when one of the subtasks of the task running the coordinated operator failed.
	 */
	void subtaskFailed(int subtask, @Nullable Throwable reason);

	// ------------------------------------------------------------------------

	CompletableFuture<byte[]> checkpointCoordinator(long checkpointId) throws Exception;

	/**
	 * Notifies the coordinator that the checkpoint with the given checkpointId completes and
	 * was committed.
	 *
	 * <p><b>Important:</b> This method is not supposed to throw an exception, because by the
	 * time we notify that the checkpoint is complete, the checkpoint is committed and cannot be
	 * aborted any more. If the coordinator gets into an inconsistent state internally, it should
	 * fail the job ({@link Context#failJob(Throwable)}) instead. Any exception propagating from
	 * this method may be treated as a fatal error for the JobManager, crashing the JobManager,
	 * and leading to an expensive "master failover" procedure.
	 */
	void checkpointComplete(long checkpointId);

	void resetToCheckpoint(byte[] checkpointData) throws Exception;

	// ------------------------------------------------------------------------
	// ------------------------------------------------------------------------

	/**
	 * The context gives the OperatorCoordinator access to contextual information and provides a
	 * gateway to interact with other components, such as sending operator events.
	 */
	interface Context {

		OperatorID getOperatorId();

		CompletableFuture<Acknowledge> sendEvent(OperatorEvent evt, int targetSubtask) throws TaskNotRunningException;

		void failTask(int subtask, Throwable cause);

		void failJob(Throwable cause);

		int currentParallelism();
	}

	// ------------------------------------------------------------------------

	/**
	 * The provider creates an OperatorCoordinator and takes a {@link Context} to pass to the OperatorCoordinator.
	 * This method is, for example, called on the job manager when the scheduler and execution graph are
	 * created, to instantiate the OperatorCoordinator.
	 *
	 * <p>The factory is {@link Serializable}, because it is attached to the JobGraph and is part
	 * of the serialized job graph that is sent to the dispatcher, or stored for recovery.
	 */
	interface Provider extends Serializable {

		OperatorID getOperatorId();

		OperatorCoordinator create(Context context);
	}
}
