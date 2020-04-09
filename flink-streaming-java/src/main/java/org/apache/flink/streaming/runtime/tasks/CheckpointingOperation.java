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
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

final class CheckpointingOperation {

	public static final Logger LOG = LoggerFactory.getLogger(CheckpointingOperation.class);

	static void execute(
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			CheckpointMetrics checkpointMetrics,
			CheckpointStreamFactory storageLocation,
			OperatorChain<?, ?> operatorChain,
			String taskName,
			CloseableRegistry closeableRegistry,
			ExecutorService threadPool,
			Environment environment,
			AsyncExceptionHandler asyncExceptionHandler,
			Supplier<Boolean> isCanceled) throws Exception {

		Preconditions.checkNotNull(checkpointMetaData);
		Preconditions.checkNotNull(checkpointOptions);
		Preconditions.checkNotNull(checkpointMetrics);
		Preconditions.checkNotNull(storageLocation);
		Preconditions.checkNotNull(operatorChain);
		Preconditions.checkNotNull(closeableRegistry);
		Preconditions.checkNotNull(threadPool);
		Preconditions.checkNotNull(environment);
		Preconditions.checkNotNull(asyncExceptionHandler);

		long startSyncPartNano = System.nanoTime();

		HashMap<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress = new HashMap<>(operatorChain.getNumberOfOperators());
		try {
			for (StreamOperatorWrapper<?, ?> operatorWrapper : operatorChain.getAllOperators(true)) {
				StreamOperator<?> op = operatorWrapper.getStreamOperator();
				OperatorSnapshotFutures snapshotInProgress = checkpointStreamOperator(
					op,
					checkpointMetaData,
					checkpointOptions,
					storageLocation,
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
			threadPool.execute(new AsyncCheckpointRunnable(
				operatorSnapshotsInProgress,
				checkpointMetaData,
				checkpointMetrics,
				startAsyncPartNano,
				taskName,
				closeableRegistry,
				environment,
				asyncExceptionHandler));

			if (LOG.isDebugEnabled()) {
				LOG.debug("{} - finished synchronous part of checkpoint {}. " +
						"Alignment duration: {} ms, snapshot duration {} ms",
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
				LOG.debug("{} - did NOT finish synchronous part of checkpoint {}. " +
						"Alignment duration: {} ms, snapshot duration {} ms",
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
				environment.declineCheckpoint(checkpointMetaData.getCheckpointId(), ex);
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
