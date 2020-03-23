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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStorageWorkerView;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

class SubtaskCheckpointCoordinatorImpl implements SubtaskCheckpointCoordinator {
	private static final Logger LOG = LoggerFactory.getLogger(SubtaskCheckpointCoordinatorImpl.class);

	private final CheckpointStorageWorkerView checkpointStorage;
	private final String taskName;
	private final CloseableRegistry closeableRegistry;
	private final ExecutorService executorService;
	private final Environment env;
	private final AsyncExceptionHandler asyncExceptionHandler;
	private final StreamTaskActionExecutor actionExecutor;

	SubtaskCheckpointCoordinatorImpl(
			CheckpointStorageWorkerView checkpointStorage,
			String taskName,
			StreamTaskActionExecutor actionExecutor,
			CloseableRegistry closeableRegistry,
			ExecutorService executorService,
			Environment env,
			AsyncExceptionHandler asyncExceptionHandler) {
		this.checkpointStorage = checkNotNull(checkpointStorage);
		this.taskName = checkNotNull(taskName);
		this.closeableRegistry = checkNotNull(closeableRegistry);
		this.executorService = checkNotNull(executorService);
		this.env = checkNotNull(env);
		this.asyncExceptionHandler = checkNotNull(asyncExceptionHandler);
		this.actionExecutor = checkNotNull(actionExecutor);
	}

	@Override
	public void abortCheckpointOnBarrier(long checkpointId, Throwable cause, OperatorChain<?, ?> operatorChain) throws Exception {
		LOG.debug("Aborting checkpoint via cancel-barrier {} for task {}", checkpointId, taskName);

		// notify the coordinator that we decline this checkpoint
		env.declineCheckpoint(checkpointId, cause);

		// notify all downstream operators that they should not wait for a barrier from us
		actionExecutor.runThrowing(() -> operatorChain.broadcastCheckpointCancelMarker(checkpointId));
	}

	@Override
	public CheckpointStorageWorkerView getCheckpointStorage() {
		return checkpointStorage;
	}

	@Override
	public void checkpointState(
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			CheckpointMetrics checkpointMetrics,
			OperatorChain<?, ?> operatorChain,
			Supplier<Boolean> isCanceled) throws Exception {
		checkNotNull(checkpointOptions);
		checkNotNull(checkpointMetrics);
		final long checkpointId = checkpointMetaData.getCheckpointId();

		// All of the following steps happen as an atomic step from the perspective of barriers and
		// records/watermarks/timers/callbacks.
		// We generally try to emit the checkpoint barrier as soon as possible to not affect downstream
		// checkpoint alignments

		// Step (1): Prepare the checkpoint, allow operators to do some pre-barrier work.
		//           The pre-barrier work should be nothing or minimal in the common case.
		operatorChain.prepareSnapshotPreBarrier(checkpointId);

		// Step (2): Send the checkpoint barrier downstream
		operatorChain.broadcastCheckpointBarrier(
			checkpointId,
			checkpointMetaData.getTimestamp(),
			checkpointOptions);

		// Step (3): Take the state snapshot. This should be largely asynchronous, to not
		//           impact progress of the streaming topology

		CheckpointStreamFactory storage = checkpointStorage.resolveCheckpointStorageLocation(
			checkpointMetaData.getCheckpointId(),
			checkpointOptions.getTargetLocation());

		long startSyncPartNano = System.nanoTime();

		HashMap<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress = new HashMap<>(operatorChain.getNumberOfOperators());
		try {
			for (StreamOperatorWrapper<?, ?> operatorWrapper : operatorChain.getAllOperators(true)) {
				StreamOperator<?> op = operatorWrapper.getStreamOperator();
				OperatorSnapshotFutures snapshotInProgress = checkpointStreamOperator(
					op,
					checkpointMetaData,
					checkpointOptions,
					storage,
					isCanceled);
				operatorSnapshotsInProgress.put(op.getOperatorID(), snapshotInProgress);
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("Finished synchronous checkpoints for checkpoint {} on task {}",
					checkpointMetaData.getCheckpointId(), taskName);
			}

			long startAsyncPartNano = System.nanoTime();

			checkpointMetrics.setSyncDurationMillis((startAsyncPartNano - startSyncPartNano) / 1_000_000);

			// we are transferring ownership over snapshotInProgressList for cleanup to the thread, active on submit
			executorService.execute(new AsyncCheckpointRunnable(
				operatorSnapshotsInProgress,
				checkpointMetaData,
				checkpointMetrics,
				startAsyncPartNano,
				taskName,
				closeableRegistry,
				env,
				asyncExceptionHandler));

			if (LOG.isDebugEnabled()) {
				LOG.debug(
					"{} - finished synchronous part of checkpoint {}. Alignment duration: {} ms, snapshot duration {} ms",
					taskName, checkpointMetaData.getCheckpointId(),
					checkpointMetrics.getAlignmentDurationNanos() / 1_000_000,
					checkpointMetrics.getSyncDurationMillis());
			}
		} catch (Exception ex) {
			// Cleanup to release resources
			for (OperatorSnapshotFutures operatorSnapshotResult : operatorSnapshotsInProgress.values()) {
				if (null != operatorSnapshotResult) {
					try {
						operatorSnapshotResult.cancel();
					} catch (Exception e) {
						LOG.warn("Could not properly cancel an operator snapshot result.", e);
					}
				}
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug(
					"{} - did NOT finish synchronous part of checkpoint {}. Alignment duration: {} ms, snapshot duration {} ms",
					taskName, checkpointMetaData.getCheckpointId(),
					checkpointMetrics.getAlignmentDurationNanos() / 1_000_000,
					checkpointMetrics.getSyncDurationMillis());
			}

			if (checkpointOptions.getCheckpointType().isSynchronous()) {
				// in the case of a synchronous checkpoint, we always rethrow the exception,
				// so that the task fails.
				// this is because the intention is always to stop the job after this checkpointing
				// operation, and without the failure, the task would go back to normal execution.
				throw ex;
			} else {
				env.declineCheckpoint(checkpointMetaData.getCheckpointId(), ex);
			}
		}
	}

	private static OperatorSnapshotFutures checkpointStreamOperator(
			StreamOperator<?> op,
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			CheckpointStreamFactory storageLocation,
			Supplier<Boolean> isCanceled) throws Exception {
		try {
			return op.snapshotState(
				checkpointMetaData.getCheckpointId(),
				checkpointMetaData.getTimestamp(),
				checkpointOptions,
				storageLocation);
		}
		catch (Exception ex) {
			if (!isCanceled.get()) {
				LOG.info(ex.getMessage(), ex);
			}
			throw ex;
		}
	}
}
