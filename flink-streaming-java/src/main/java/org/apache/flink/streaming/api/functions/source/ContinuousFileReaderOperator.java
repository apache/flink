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

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.CheckpointableInputFormat;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OutputTypeConfigurable;
import org.apache.flink.streaming.api.operators.StreamSourceContexts;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The operator that reads the {@link TimestampedFileInputSplit splits} received from the preceding
 * {@link ContinuousFileMonitoringFunction}. Contrary to the {@link ContinuousFileMonitoringFunction}
 * which has a parallelism of 1, this operator can have DOP > 1.
 *
 * <p>As soon as a split descriptor is received, it is put in a queue, and have another
 * thread read the actual data of the split. This architecture allows the separation of the
 * reading thread from the one emitting the checkpoint barriers, thus removing any potential
 * back-pressure.
 */
@Internal
public class ContinuousFileReaderOperator<OUT> extends AbstractStreamOperator<OUT>
	implements OneInputStreamOperator<TimestampedFileInputSplit, OUT>, OutputTypeConfigurable<OUT>, BoundedOneInput {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(ContinuousFileReaderOperator.class);

	private FileInputFormat<OUT> format;
	private TypeSerializer<OUT> serializer;

	private transient Object checkpointLock;

	private transient SplitReader<OUT> reader;
	private transient SourceFunction.SourceContext<OUT> readerContext;

	private transient ListState<TimestampedFileInputSplit> checkpointedState;
	private transient List<TimestampedFileInputSplit> restoredReaderState;

	public ContinuousFileReaderOperator(FileInputFormat<OUT> format) {
		this.format = checkNotNull(format);
	}

	@Override
	public void setOutputType(TypeInformation<OUT> outTypeInfo, ExecutionConfig executionConfig) {
		this.serializer = outTypeInfo.createSerializer(executionConfig);
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);

		checkState(checkpointedState == null,	"The reader state has already been initialized.");

		checkpointedState = context.getOperatorStateStore().getSerializableListState("splits");

		int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();
		if (context.isRestored()) {
			LOG.info("Restoring state for the {} (taskIdx={}).", getClass().getSimpleName(), subtaskIdx);

			// this may not be null in case we migrate from a previous Flink version.
			if (restoredReaderState == null) {
				restoredReaderState = new ArrayList<>();
				for (TimestampedFileInputSplit split : checkpointedState.get()) {
					restoredReaderState.add(split);
				}

				if (LOG.isDebugEnabled()) {
					LOG.debug("{} (taskIdx={}) restored {}.", getClass().getSimpleName(), subtaskIdx, restoredReaderState);
				}
			}
		} else {
			LOG.info("No state to restore for the {} (taskIdx={}).", getClass().getSimpleName(), subtaskIdx);
		}
	}

	@Override
	public void open() throws Exception {
		super.open();

		checkState(this.reader == null, "The reader is already initialized.");
		checkState(this.serializer != null, "The serializer has not been set. " +
			"Probably the setOutputType() was not called. Please report it.");

		this.format.setRuntimeContext(getRuntimeContext());
		this.format.configure(new Configuration());
		this.checkpointLock = getContainingTask().getCheckpointLock();

		// set the reader context based on the time characteristic
		final TimeCharacteristic timeCharacteristic = getOperatorConfig().getTimeCharacteristic();
		final long watermarkInterval = getRuntimeContext().getExecutionConfig().getAutoWatermarkInterval();
		this.readerContext = StreamSourceContexts.getSourceContext(
			timeCharacteristic,
			getProcessingTimeService(),
			checkpointLock,
			getContainingTask().getStreamStatusMaintainer(),
			output,
			watermarkInterval,
			-1);

		// and initialize the split reading thread
		this.reader = new SplitReader<>(format, serializer, readerContext, checkpointLock, restoredReaderState);
		this.restoredReaderState = null;
		this.reader.start();
	}

	@Override
	public void processElement(StreamRecord<TimestampedFileInputSplit> element) throws Exception {
		reader.addSplit(element.getValue());
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		// we do nothing because we emit our own watermarks if needed.
	}

	@Override
	public void dispose() throws Exception {
		super.dispose();

		// first try to cancel it properly and
		// give it some time until it finishes
		reader.cancel();
		try {
			reader.join(200);
		} catch (InterruptedException e) {
			// we can ignore this
		}

		// if the above did not work, then interrupt the thread repeatedly
		while (reader.isAlive()) {

			StringBuilder bld = new StringBuilder();
			StackTraceElement[] stack = reader.getStackTrace();
			for (StackTraceElement e : stack) {
				bld.append(e).append('\n');
			}
			LOG.warn("The reader is stuck in method:\n {}", bld.toString());

			reader.interrupt();
			try {
				reader.join(50);
			} catch (InterruptedException e) {
				// we can ignore this
			}
		}
		reader = null;
		readerContext = null;
		restoredReaderState = null;
		format = null;
		serializer = null;
	}

	@Override
	public void close() throws Exception {
		super.close();

		waitSplitReaderFinished();

		output.close();
	}

	@Override
	public void endInput() throws Exception {
		waitSplitReaderFinished();
	}

	private void waitSplitReaderFinished() throws InterruptedException {
		// make sure that we hold the checkpointing lock
		Thread.holdsLock(checkpointLock);

		// close the reader to signal that no more splits will come. By doing this,
		// the reader will exit as soon as it finishes processing the already pending splits.
		// This method will wait until then. Further cleaning up is handled by the dispose().

		while (reader != null && reader.isAlive() && reader.isRunning()) {
			reader.close();
			checkpointLock.wait();
		}

		// finally if we are operating on event or ingestion time,
		// emit the long-max watermark indicating the end of the stream,
		// like a normal source would do.

		if (readerContext != null) {
			readerContext.emitWatermark(Watermark.MAX_WATERMARK);
			readerContext.close();
			readerContext = null;
		}
	}

	private class SplitReader<OT> extends Thread {

		private volatile boolean shouldClose;

		private volatile boolean isRunning;

		private final FileInputFormat<OT> format;
		private final TypeSerializer<OT> serializer;

		private final Object checkpointLock;
		private final SourceFunction.SourceContext<OT> readerContext;

		private final Queue<TimestampedFileInputSplit> pendingSplits;

		private TimestampedFileInputSplit currentSplit;

		private volatile boolean isSplitOpen;

		private SplitReader(FileInputFormat<OT> format,
					TypeSerializer<OT> serializer,
					SourceFunction.SourceContext<OT> readerContext,
					Object checkpointLock,
					List<TimestampedFileInputSplit> restoredState) {

			this.format = checkNotNull(format, "Unspecified FileInputFormat.");
			this.serializer = checkNotNull(serializer, "Unspecified Serializer.");
			this.readerContext = checkNotNull(readerContext, "Unspecified Reader Context.");
			this.checkpointLock = checkNotNull(checkpointLock, "Unspecified checkpoint lock.");

			this.shouldClose = false;
			this.isRunning = true;

			this.pendingSplits = new PriorityQueue<>();

			// this is the case where a task recovers from a previous failed attempt
			if (restoredState != null) {
				this.pendingSplits.addAll(restoredState);
			}
		}

		private void addSplit(TimestampedFileInputSplit split) {
			checkNotNull(split, "Cannot insert a null value in the pending splits queue.");
			synchronized (checkpointLock) {
				this.pendingSplits.add(split);
			}
		}

		public boolean isRunning() {
			return this.isRunning;
		}

		@Override
		public void run() {
			try {

				Counter completedSplitsCounter = getMetricGroup().counter("numSplitsProcessed");
				this.format.openInputFormat();

				while (this.isRunning) {

					synchronized (checkpointLock) {

						if (currentSplit == null) {
							currentSplit = this.pendingSplits.poll();

							// if the list of pending splits is empty (currentSplit == null) then:
							//   1) if close() was called on the operator then exit the while loop
							//   2) if not wait 50 ms and try again to fetch a new split to read

							if (currentSplit == null) {
								if (this.shouldClose) {
									isRunning = false;
								} else {
									checkpointLock.wait(50);
								}
								continue;
							}
						}

						if (this.format instanceof CheckpointableInputFormat && currentSplit.getSplitState() != null) {
							// recovering after a node failure with an input
							// format that supports resetting the offset
							((CheckpointableInputFormat<TimestampedFileInputSplit, Serializable>) this.format).
								reopen(currentSplit, currentSplit.getSplitState());
						} else {
							// we either have a new split, or we recovered from a node
							// failure but the input format does not support resetting the offset.
							this.format.open(currentSplit);
						}

						// reset the restored state to null for the next iteration
						this.currentSplit.resetSplitState();
						this.isSplitOpen = true;
					}

					LOG.debug("Reading split: " + currentSplit);

					try {
						OT nextElement = serializer.createInstance();
						while (!format.reachedEnd()) {
							synchronized (checkpointLock) {
								nextElement = format.nextRecord(nextElement);
								if (nextElement != null) {
									readerContext.collect(nextElement);
								} else {
									break;
								}
							}
						}
						completedSplitsCounter.inc();

					} finally {
						// close and prepare for the next iteration
						synchronized (checkpointLock) {
							this.format.close();
							this.isSplitOpen = false;
							this.currentSplit = null;
						}
					}
				}

			} catch (Throwable e) {

				getContainingTask().handleAsyncException("Caught exception when processing split: " + currentSplit, e);

			} finally {
				synchronized (checkpointLock) {
					LOG.debug("Reader terminated, and exiting...");

					try {
						this.format.closeInputFormat();
					} catch (IOException e) {
						getContainingTask().handleAsyncException(
							"Caught exception from " + this.format.getClass().getName() + ".closeInputFormat() : " + e.getMessage(), e);
					}
					this.isSplitOpen = false;
					this.currentSplit = null;
					this.isRunning = false;

					checkpointLock.notifyAll();
				}
			}
		}

		private List<TimestampedFileInputSplit> getReaderState() throws IOException {
			List<TimestampedFileInputSplit> snapshot = new ArrayList<>(this.pendingSplits.size());
			if (currentSplit != null) {
				if (this.format instanceof CheckpointableInputFormat && this.isSplitOpen) {
					Serializable formatState =
						((CheckpointableInputFormat<TimestampedFileInputSplit, Serializable>) this.format).getCurrentState();
					this.currentSplit.setSplitState(formatState);
				}
				snapshot.add(this.currentSplit);
			}
			snapshot.addAll(this.pendingSplits);
			return snapshot;
		}

		public void cancel() {
			this.isRunning = false;
		}

		public void close() {
			this.shouldClose = true;
		}
	}

	//	---------------------			Checkpointing			--------------------------

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		super.snapshotState(context);

		checkState(checkpointedState != null,
			"The operator state has not been properly initialized.");

		int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();

		checkpointedState.clear();

		List<TimestampedFileInputSplit> readerState = reader.getReaderState();

		try {
			for (TimestampedFileInputSplit split : readerState) {
				// create a new partition for each entry.
				checkpointedState.add(split);
			}
		} catch (Exception e) {
			checkpointedState.clear();

			throw new Exception("Could not add timestamped file input splits to to operator " +
				"state backend of operator " + getOperatorName() + '.', e);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("{} (taskIdx={}) checkpointed {} splits: {}.",
				getClass().getSimpleName(), subtaskIdx, readerState.size(), readerState);
		}
	}
}
