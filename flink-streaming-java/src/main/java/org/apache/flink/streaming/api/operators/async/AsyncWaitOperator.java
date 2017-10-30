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

package org.apache.flink.streaming.api.operators.async;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.AsyncDataStream.OutputMode;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.async.queue.OrderedStreamElementQueue;
import org.apache.flink.streaming.api.operators.async.queue.StreamElementQueue;
import org.apache.flink.streaming.api.operators.async.queue.StreamElementQueueEntry;
import org.apache.flink.streaming.api.operators.async.queue.StreamRecordQueueEntry;
import org.apache.flink.streaming.api.operators.async.queue.UnorderedStreamElementQueue;
import org.apache.flink.streaming.api.operators.async.queue.WatermarkQueueEntry;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The {@link AsyncWaitOperator} allows to asynchronously process incoming stream records. For that
 * the operator creates an {@link ResultFuture} which is passed to an {@link AsyncFunction}.
 * Within the async function, the user can complete the async collector arbitrarily. Once the async
 * collector has been completed, the result is emitted by the operator's emitter to downstream
 * operators.
 *
 * <p>The operator offers different output modes depending on the chosen
 * {@link OutputMode}. In order to give exactly once processing guarantees, the
 * operator stores all currently in-flight {@link StreamElement} in it's operator state. Upon
 * recovery the recorded set of stream elements is replayed.
 *
 * <p>In case of chaining of this operator, it has to be made sure that the operators in the chain are
 * opened tail to head. The reason for this is that an opened {@link AsyncWaitOperator} starts
 * already emitting recovered {@link StreamElement} to downstream operators.
 *
 * @param <IN> Input type for the operator.
 * @param <OUT> Output type for the operator.
 */
@Internal
public class AsyncWaitOperator<IN, OUT>
		extends AbstractUdfStreamOperator<OUT, AsyncFunction<IN, OUT>>
		implements OneInputStreamOperator<IN, OUT>, OperatorActions {
	private static final long serialVersionUID = 1L;

	private static final String STATE_NAME = "_async_wait_operator_state_";

	/** Capacity of the stream element queue. */
	private final int capacity;

	/** Output mode for this operator. */
	private final AsyncDataStream.OutputMode outputMode;

	/** Timeout for the async collectors. */
	private final long timeout;

	protected transient Object checkpointingLock;

	/** {@link TypeSerializer} for inputs while making snapshots. */
	private transient StreamElementSerializer<IN> inStreamElementSerializer;

	/** Recovered input stream elements. */
	private transient ListState<StreamElement> recoveredStreamElements;

	/** Queue to store the currently in-flight stream elements into. */
	private transient StreamElementQueue queue;

	/** Pending stream element which could not yet added to the queue. */
	private transient StreamElementQueueEntry<?> pendingStreamElementQueueEntry;

	private transient ExecutorService executor;

	/** Emitter for the completed stream element queue entries. */
	private transient Emitter<OUT> emitter;

	/** Thread running the emitter. */
	private transient Thread emitterThread;

	public AsyncWaitOperator(
			AsyncFunction<IN, OUT> asyncFunction,
			long timeout,
			int capacity,
			AsyncDataStream.OutputMode outputMode) {
		super(asyncFunction);
		chainingStrategy = ChainingStrategy.ALWAYS;

		Preconditions.checkArgument(capacity > 0, "The number of concurrent async operation should be greater than 0.");
		this.capacity = capacity;

		this.outputMode = Preconditions.checkNotNull(outputMode, "outputMode");

		this.timeout = timeout;
	}

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output) {
		super.setup(containingTask, config, output);

		this.checkpointingLock = getContainingTask().getCheckpointLock();

		this.inStreamElementSerializer = new StreamElementSerializer<>(
			getOperatorConfig().<IN>getTypeSerializerIn1(getUserCodeClassloader()));

		// create the operators executor for the complete operations of the queue entries
		this.executor = Executors.newSingleThreadExecutor();

		switch (outputMode) {
			case ORDERED:
				queue = new OrderedStreamElementQueue(
					capacity,
					executor,
					this);
				break;
			case UNORDERED:
				queue = new UnorderedStreamElementQueue(
					capacity,
					executor,
					this);
				break;
			default:
				throw new IllegalStateException("Unknown async mode: " + outputMode + '.');
		}
	}

	@Override
	public void open() throws Exception {
		super.open();

		// create the emitter
		this.emitter = new Emitter<>(checkpointingLock, output, queue, this);

		// start the emitter thread
		this.emitterThread = new Thread(emitter, "AsyncIO-Emitter-Thread (" + getOperatorName() + ')');
		emitterThread.setDaemon(true);
		emitterThread.start();

		// process stream elements from state, since the Emit thread will start as soon as all
		// elements from previous state are in the StreamElementQueue, we have to make sure that the
		// order to open all operators in the operator chain proceeds from the tail operator to the
		// head operator.
		if (recoveredStreamElements != null) {
			for (StreamElement element : recoveredStreamElements.get()) {
				if (element.isRecord()) {
					processElement(element.<IN>asRecord());
				}
				else if (element.isWatermark()) {
					processWatermark(element.asWatermark());
				}
				else if (element.isLatencyMarker()) {
					processLatencyMarker(element.asLatencyMarker());
				}
				else {
					throw new IllegalStateException("Unknown record type " + element.getClass() +
						" encountered while opening the operator.");
				}
			}
			recoveredStreamElements = null;
		}

	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		final StreamRecordQueueEntry<OUT> streamRecordBufferEntry = new StreamRecordQueueEntry<>(element);

		if (timeout > 0L) {
			// register a timeout for this AsyncStreamRecordBufferEntry
			long timeoutTimestamp = timeout + getProcessingTimeService().getCurrentProcessingTime();

			final ScheduledFuture<?> timerFuture = getProcessingTimeService().registerTimer(
				timeoutTimestamp,
				new ProcessingTimeCallback() {
					@Override
					public void onProcessingTime(long timestamp) throws Exception {
						streamRecordBufferEntry.completeExceptionally(
							new TimeoutException("Async function call has timed out."));
					}
				});

			// Cancel the timer once we've completed the stream record buffer entry. This will remove
			// the register trigger task
			streamRecordBufferEntry.onComplete(
				(StreamElementQueueEntry<Collection<OUT>> value) -> {
					timerFuture.cancel(true);
				},
				executor);
		}

		addAsyncBufferEntry(streamRecordBufferEntry);

		userFunction.asyncInvoke(element.getValue(), streamRecordBufferEntry);
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		WatermarkQueueEntry watermarkBufferEntry = new WatermarkQueueEntry(mark);

		addAsyncBufferEntry(watermarkBufferEntry);
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		super.snapshotState(context);

		ListState<StreamElement> partitionableState =
			getOperatorStateBackend().getListState(new ListStateDescriptor<>(STATE_NAME, inStreamElementSerializer));
		partitionableState.clear();

		Collection<StreamElementQueueEntry<?>> values = queue.values();

		try {
			for (StreamElementQueueEntry<?> value : values) {
				partitionableState.add(value.getStreamElement());
			}

			// add the pending stream element queue entry if the stream element queue is currently full
			if (pendingStreamElementQueueEntry != null) {
				partitionableState.add(pendingStreamElementQueueEntry.getStreamElement());
			}
		} catch (Exception e) {
			partitionableState.clear();

			throw new Exception("Could not add stream element queue entries to operator state " +
				"backend of operator " + getOperatorName() + '.', e);
		}
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);
		recoveredStreamElements = context
			.getOperatorStateStore()
			.getListState(new ListStateDescriptor<>(STATE_NAME, inStreamElementSerializer));

	}

	@Override
	public void close() throws Exception {
		try {
			assert(Thread.holdsLock(checkpointingLock));

			while (!queue.isEmpty()) {
				// wait for the emitter thread to output the remaining elements
				// for that he needs the checkpointing lock and thus we have to free it
				checkpointingLock.wait();
			}
		}
		finally {
			Exception exception = null;

			try {
				super.close();
			} catch (InterruptedException interrupted) {
				exception = interrupted;

				Thread.currentThread().interrupt();
			} catch (Exception e) {
				exception = e;
			}

			try {
				// terminate the emitter, the emitter thread and the executor
				stopResources(true);
			} catch (InterruptedException interrupted) {
				exception = ExceptionUtils.firstOrSuppressed(interrupted, exception);

				Thread.currentThread().interrupt();
			} catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}

			if (exception != null) {
				LOG.warn("Errors occurred while closing the AsyncWaitOperator.", exception);
			}
		}
	}

	@Override
	public void dispose() throws Exception {
		Exception exception = null;

		try {
			super.dispose();
		} catch (InterruptedException interrupted) {
			exception = interrupted;

			Thread.currentThread().interrupt();
		} catch (Exception e) {
			exception = e;
		}

		try {
			stopResources(false);
		} catch (InterruptedException interrupted) {
			exception = ExceptionUtils.firstOrSuppressed(interrupted, exception);

			Thread.currentThread().interrupt();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		if (exception != null) {
			throw exception;
		}
	}

	/**
	 * Close the operator's resources. They include the emitter thread and the executor to run
	 * the queue's complete operation.
	 *
	 * @param waitForShutdown is true if the method should wait for the resources to be freed;
	 *                           otherwise false.
	 * @throws InterruptedException if current thread has been interrupted
	 */
	private void stopResources(boolean waitForShutdown) throws InterruptedException {
		emitter.stop();
		emitterThread.interrupt();

		executor.shutdown();

		if (waitForShutdown) {
			try {
				if (!executor.awaitTermination(365L, TimeUnit.DAYS)) {
					executor.shutdownNow();
				}
			} catch (InterruptedException e) {
				executor.shutdownNow();

				Thread.currentThread().interrupt();
			}

			/*
			 * FLINK-5638: If we have the checkpoint lock we might have to free it for a while so
			 * that the emitter thread can complete/react to the interrupt signal.
			 */
			if (Thread.holdsLock(checkpointingLock)) {
				while (emitterThread.isAlive()) {
					checkpointingLock.wait(100L);
				}
			}

			emitterThread.join();
		} else {
			executor.shutdownNow();
		}
	}

	/**
	 * Add the given stream element queue entry to the operator's stream element queue. This
	 * operation blocks until the element has been added.
	 *
	 * <p>For that it tries to put the element into the queue and if not successful then it waits on
	 * the checkpointing lock. The checkpointing lock is also used by the {@link Emitter} to output
	 * elements. The emitter is also responsible for notifying this method if the queue has capacity
	 * left again, by calling notifyAll on the checkpointing lock.
	 *
	 * @param streamElementQueueEntry to add to the operator's queue
	 * @param <T> Type of the stream element queue entry's result
	 * @throws InterruptedException if the current thread has been interrupted
	 */
	private <T> void addAsyncBufferEntry(StreamElementQueueEntry<T> streamElementQueueEntry) throws InterruptedException {
		assert(Thread.holdsLock(checkpointingLock));

		pendingStreamElementQueueEntry = streamElementQueueEntry;

		while (!queue.tryPut(streamElementQueueEntry)) {
			// we wait for the emitter to notify us if the queue has space left again
			checkpointingLock.wait();
		}

		pendingStreamElementQueueEntry = null;
	}

	@Override
	public void failOperator(Throwable throwable) {
		getContainingTask().getEnvironment().failExternally(throwable);
	}
}
