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
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter.ChannelStateWriteResult;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriterImpl;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointStorageWorkerView;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.BiFunctionWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import static org.apache.flink.runtime.checkpoint.CheckpointType.CHECKPOINT;
import static org.apache.flink.util.Preconditions.checkNotNull;

class SubtaskCheckpointCoordinatorImpl implements SubtaskCheckpointCoordinator {

	private static final Logger LOG = LoggerFactory.getLogger(SubtaskCheckpointCoordinatorImpl.class);

	private final CachingCheckpointStorageWorkerView checkpointStorage;
	private final String taskName;
	private final CloseableRegistry closeableRegistry;
	private final ExecutorService executorService;
	private final Environment env;
	private final AsyncExceptionHandler asyncExceptionHandler;
	private final ChannelStateWriter channelStateWriter;
	private final StreamTaskActionExecutor actionExecutor;
	private final boolean unalignedCheckpointEnabled;
	private final BiFunctionWithException<ChannelStateWriter, Long, CompletableFuture<Void>, IOException> prepareInputSnapshot;

	SubtaskCheckpointCoordinatorImpl(
			CheckpointStorageWorkerView checkpointStorage,
			String taskName,
			StreamTaskActionExecutor actionExecutor,
			CloseableRegistry closeableRegistry,
			ExecutorService executorService,
			Environment env,
			AsyncExceptionHandler asyncExceptionHandler,
			boolean unalignedCheckpointEnabled,
			BiFunctionWithException<ChannelStateWriter, Long, CompletableFuture<Void>, IOException> prepareInputSnapshot) throws IOException {
		this.checkpointStorage = new CachingCheckpointStorageWorkerView(checkNotNull(checkpointStorage));
		this.taskName = checkNotNull(taskName);
		this.closeableRegistry = checkNotNull(closeableRegistry);
		this.executorService = checkNotNull(executorService);
		this.env = checkNotNull(env);
		this.asyncExceptionHandler = checkNotNull(asyncExceptionHandler);
		this.actionExecutor = checkNotNull(actionExecutor);
		this.channelStateWriter = unalignedCheckpointEnabled ? openChannelStateWriter() : ChannelStateWriter.NO_OP;
		this.unalignedCheckpointEnabled = unalignedCheckpointEnabled;
		this.prepareInputSnapshot = prepareInputSnapshot;
		this.closeableRegistry.registerCloseable(this);
	}

	private ChannelStateWriter openChannelStateWriter() {
		ChannelStateWriterImpl writer = new ChannelStateWriterImpl(this.checkpointStorage);
		writer.open();
		return writer;
	}

	@Override
	public void abortCheckpointOnBarrier(long checkpointId, Throwable cause, OperatorChain<?, ?> operatorChain) throws IOException {
		LOG.debug("Aborting checkpoint via cancel-barrier {} for task {}", checkpointId, taskName);

		checkpointStorage.clearCacheFor(checkpointId);

		channelStateWriter.abort(checkpointId, cause);

		// notify the coordinator that we decline this checkpoint
		env.declineCheckpoint(checkpointId, cause);

		// notify all downstream operators that they should not wait for a barrier from us
		actionExecutor.runThrowing(() -> operatorChain.broadcastEvent(new CancelCheckpointMarker(checkpointId)));
	}

	@Override
	public CheckpointStorageWorkerView getCheckpointStorage() {
		return checkpointStorage;
	}

	@Override
	public ChannelStateWriter getChannelStateWriter() {
		return channelStateWriter;
	}

	@Override
	public void checkpointState(
			CheckpointMetaData metadata,
			CheckpointOptions options,
			CheckpointMetrics metrics,
			OperatorChain<?, ?> operatorChain,
			Supplier<Boolean> isCanceled) throws Exception {

		checkNotNull(options);
		checkNotNull(metrics);

		// All of the following steps happen as an atomic step from the perspective of barriers and
		// records/watermarks/timers/callbacks.
		// We generally try to emit the checkpoint barrier as soon as possible to not affect downstream
		// checkpoint alignments

		// Step (1): Prepare the checkpoint, allow operators to do some pre-barrier work.
		//           The pre-barrier work should be nothing or minimal in the common case.
		operatorChain.prepareSnapshotPreBarrier(metadata.getCheckpointId());

		// Step (2): Send the checkpoint barrier downstream
		operatorChain.broadcastEvent(
			new CheckpointBarrier(metadata.getCheckpointId(), metadata.getTimestamp(), options),
			unalignedCheckpointEnabled);

		// Step (3): Prepare to spill the in-flight buffers for input and output
		if (unalignedCheckpointEnabled && !options.getCheckpointType().isSavepoint()) {
			prepareInflightDataSnapshot(metadata.getCheckpointId());
		}

		// Step (4): Take the state snapshot. This should be largely asynchronous, to not impact progress of the
		// streaming topology

		Map<OperatorID, OperatorSnapshotFutures> snapshotFutures = new HashMap<>(operatorChain.getNumberOfOperators());
		try {
			takeSnapshotSync(snapshotFutures, metadata, metrics, options, operatorChain, isCanceled);
			finishAndReportAsync(snapshotFutures, metadata, metrics, options);
		} catch (Exception ex) {
			cleanup(snapshotFutures, metadata, metrics, options, ex);
		}
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId, OperatorChain<?, ?> operatorChain, Supplier<Boolean> isRunning) throws Exception {
		if (isRunning.get()) {
			LOG.debug("Notification of complete checkpoint for task {}", taskName);

			for (StreamOperatorWrapper<?, ?> operatorWrapper : operatorChain.getAllOperators(true)) {
				operatorWrapper.getStreamOperator().notifyCheckpointComplete(checkpointId);
			}
		} else {
			LOG.debug("Ignoring notification of complete checkpoint for not-running task {}", taskName);
		}
		channelStateWriter.notifyCheckpointComplete(checkpointId);
		env.getTaskStateManager().notifyCheckpointComplete(checkpointId);
	}

	private void cleanup(
			Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress,
			CheckpointMetaData metadata,
			CheckpointMetrics metrics, CheckpointOptions options,
			Exception ex) throws Exception {

		for (OperatorSnapshotFutures operatorSnapshotResult : operatorSnapshotsInProgress.values()) {
			if (operatorSnapshotResult != null) {
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
				taskName, metadata.getCheckpointId(),
				metrics.getAlignmentDurationNanos() / 1_000_000,
				metrics.getSyncDurationMillis());
		}

		if (options.getCheckpointType().isSynchronous()) {
			// in the case of a synchronous checkpoint, we always rethrow the exception,
			// so that the task fails.
			// this is because the intention is always to stop the job after this checkpointing
			// operation, and without the failure, the task would go back to normal execution.
			throw ex;
		} else {
			env.declineCheckpoint(metadata.getCheckpointId(), ex);
		}
	}

	private void prepareInflightDataSnapshot(long checkpointId) throws IOException {
		prepareInputSnapshot.apply(channelStateWriter, checkpointId)
			.thenAccept(unused -> channelStateWriter.finishInput(checkpointId));

		ResultPartitionWriter[] writers = env.getAllWriters();
		for (ResultPartitionWriter writer : writers) {
			for (int i = 0; i < writer.getNumberOfSubpartitions(); i++) {
				ResultSubpartition subpartition = writer.getSubpartition(i);
				channelStateWriter.addOutputData(
					checkpointId,
					subpartition.getSubpartitionInfo(),
					ChannelStateWriter.SEQUENCE_NUMBER_UNKNOWN,
					subpartition.requestInflightBufferSnapshot().toArray(new Buffer[0]));
			}
		}
		channelStateWriter.finishOutput(checkpointId);
	}

	private void finishAndReportAsync(Map<OperatorID, OperatorSnapshotFutures> snapshotFutures, CheckpointMetaData metadata, CheckpointMetrics metrics, CheckpointOptions options) {
		final Future<?> channelWrittenFuture;
		if (unalignedCheckpointEnabled && !options.getCheckpointType().isSavepoint()) {
			ChannelStateWriteResult writeResult = channelStateWriter.getWriteResult(metadata.getCheckpointId());
			channelWrittenFuture = CompletableFuture.allOf(
				writeResult.getInputChannelStateHandles(),
				writeResult.getResultSubpartitionStateHandles());
		} else {
			channelWrittenFuture = FutureUtils.completedVoidFuture();
		}
		// we are transferring ownership over snapshotInProgressList for cleanup to the thread, active on submit
		executorService.execute(new AsyncCheckpointRunnable(
			snapshotFutures,
			metadata,
			metrics,
			channelWrittenFuture,
			System.nanoTime(),
			taskName,
			closeableRegistry,
			env,
			asyncExceptionHandler));
	}

	private void takeSnapshotSync(
			Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress,
			CheckpointMetaData checkpointMetaData,
			CheckpointMetrics checkpointMetrics,
			CheckpointOptions checkpointOptions,
			OperatorChain<?, ?> operatorChain,
			Supplier<Boolean> isCanceled) throws Exception {

		long checkpointId = checkpointMetaData.getCheckpointId();
		long started = System.nanoTime();

		ChannelStateWriteResult channelStateWriteResult = checkpointOptions.getCheckpointType() == CHECKPOINT ?
								channelStateWriter.getWriteResult(checkpointId) :
								ChannelStateWriteResult.EMPTY;

		CheckpointStreamFactory storage = checkpointStorage.resolveCheckpointStorageLocation(checkpointId, checkpointOptions.getTargetLocation());

		try {
			for (StreamOperatorWrapper<?, ?> operatorWrapper : operatorChain.getAllOperators(true)) {
				operatorSnapshotsInProgress.put(
					operatorWrapper.getStreamOperator().getOperatorID(),
					buildOperatorSnapshotFutures(
						checkpointMetaData,
						checkpointOptions,
						operatorChain,
						operatorWrapper.getStreamOperator(),
						isCanceled,
						channelStateWriteResult,
						storage));
			}
		} finally {
			checkpointStorage.clearCacheFor(checkpointId);
		}

		LOG.debug(
			"{} - finished synchronous part of checkpoint {}. Alignment duration: {} ms, snapshot duration {} ms",
			taskName,
			checkpointId,
			checkpointMetrics.getAlignmentDurationNanos() / 1_000_000,
			checkpointMetrics.getSyncDurationMillis());

		checkpointMetrics.setSyncDurationMillis((System.nanoTime() - started) / 1_000_000);
	}

	private OperatorSnapshotFutures buildOperatorSnapshotFutures(
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			OperatorChain<?, ?> operatorChain,
			StreamOperator<?> op,
			Supplier<Boolean> isCanceled,
			ChannelStateWriteResult channelStateWriteResult,
			CheckpointStreamFactory storage) throws Exception {
		OperatorSnapshotFutures snapshotInProgress = checkpointStreamOperator(
			op,
			checkpointMetaData,
			checkpointOptions,
			storage,
			isCanceled);
		if (op == operatorChain.getHeadOperator()) {
			snapshotInProgress.setInputChannelStateFuture(
				channelStateWriteResult
					.getInputChannelStateHandles()
					.thenApply(StateObjectCollection::new)
					.thenApply(SnapshotResult::of));
		}
		if (op == operatorChain.getTailOperator()) {
			snapshotInProgress.setResultSubpartitionStateFuture(
				channelStateWriteResult
					.getResultSubpartitionStateHandles()
					.thenApply(StateObjectCollection::new)
					.thenApply(SnapshotResult::of));
		}
		return snapshotInProgress;
	}

	@Override
	public void close() throws IOException {
		channelStateWriter.close();
	}

	// Caches checkpoint output stream factories to prevent multiple output stream per checkpoint.
	// This could result from requesting output stream by different entities (this and channelStateWriter)
	// We can't just pass a stream to the channelStateWriter because it can receive checkpoint call earlier than this class
	// in some unaligned checkpoints scenarios
	private static class CachingCheckpointStorageWorkerView implements CheckpointStorageWorkerView {
		private final Map<Long, CheckpointStreamFactory> cache = new ConcurrentHashMap<>();
		private final CheckpointStorageWorkerView delegate;

		private CachingCheckpointStorageWorkerView(CheckpointStorageWorkerView delegate) {
			this.delegate = delegate;
		}

		void clearCacheFor(long checkpointId) {
			cache.remove(checkpointId);
		}

		@Override
		public CheckpointStreamFactory resolveCheckpointStorageLocation(long checkpointId, CheckpointStorageLocationReference reference) {
			return cache.computeIfAbsent(checkpointId, id -> {
				try {
					return delegate.resolveCheckpointStorageLocation(checkpointId, reference);
				} catch (IOException e) {
					throw new FlinkRuntimeException(e);
				}
			});
		}

		@Override
		public CheckpointStreamFactory.CheckpointStateOutputStream createTaskOwnedStateStream() throws IOException {
			return delegate.createTaskOwnedStateStream();
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
