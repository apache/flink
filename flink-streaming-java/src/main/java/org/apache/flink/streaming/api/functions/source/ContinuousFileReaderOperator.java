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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.operators.StreamCheckpointedOperator;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OutputTypeConfigurable;
import org.apache.flink.streaming.api.operators.StreamSourceContexts;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import static org.apache.flink.streaming.api.functions.source.TimestampedFileInputSplit.EOS;
import static org.apache.flink.util.Preconditions.checkState;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The operator that reads the {@link TimestampedFileInputSplit splits} received from the preceding
 * {@link ContinuousFileMonitoringFunction}. Contrary to the {@link ContinuousFileMonitoringFunction}
 * which has a parallelism of 1, this operator can have DOP > 1.
 * <p/>
 * As soon as a split descriptor is received, it is put in a queue, and have another
 * thread read the actual data of the split. This architecture allows the separation of the
 * reading thread from the one emitting the checkpoint barriers, thus removing any potential
 * back-pressure.
 */
@Internal
public class ContinuousFileReaderOperator<OUT> extends AbstractStreamOperator<OUT>
	implements OneInputStreamOperator<TimestampedFileInputSplit, OUT>, OutputTypeConfigurable<OUT>, StreamCheckpointedOperator {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(ContinuousFileReaderOperator.class);

	private FileInputFormat<OUT> format;
	private TypeSerializer<OUT> serializer;

	private transient Object checkpointLock;

	private transient SplitReader<OUT> reader;
	private transient SourceFunction.SourceContext<OUT> readerContext;
	private List<TimestampedFileInputSplit> restoredReaderState;

	public ContinuousFileReaderOperator(FileInputFormat<OUT> format) {
		this.format = checkNotNull(format);
	}

	@Override
	public void setOutputType(TypeInformation<OUT> outTypeInfo, ExecutionConfig executionConfig) {
		this.serializer = outTypeInfo.createSerializer(executionConfig);
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
			timeCharacteristic, getProcessingTimeService(), checkpointLock, output, watermarkInterval);

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

		// signal that no more splits will come, wait for the reader to finish
		// and close the collector. Further cleaning up is handled by the dispose().

		if (reader != null && reader.isAlive() && reader.isRunning()) {
			// add a dummy element to signal that no more splits will
			// arrive and wait until the reader finishes
			reader.addSplit(EOS);

			// we already have the checkpoint lock because close() is
			// called by the StreamTask while having it.
			checkpointLock.wait();
		}

		// finally if we are closed normally and we are operating on
		// event or ingestion time, emit the max watermark indicating
		// the end of the stream, like a normal source would do.

		if (readerContext != null) {
			readerContext.emitWatermark(Watermark.MAX_WATERMARK);
			readerContext.close();
		}
		output.close();
	}

	private class SplitReader<OT> extends Thread {

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

			this.isRunning = true;

			this.pendingSplits = new PriorityQueue<>(10, new Comparator<TimestampedFileInputSplit>() {
				@Override
				public int compare(TimestampedFileInputSplit o1, TimestampedFileInputSplit o2) {
					return o1.compareTo(o2);
				}
			});

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
							if (currentSplit == null) {
								checkpointLock.wait(50);
								continue;
							}
						}

						if (currentSplit.equals(EOS)) {
							isRunning = false;
							break;
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

					LOG.info("Reading split: " + currentSplit);

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
					LOG.info("Reader terminated, and exiting...");

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
			if (currentSplit != null ) {
				if (this.format instanceof CheckpointableInputFormat && this.isSplitOpen) {
					Serializable formatState = ((CheckpointableInputFormat<TimestampedFileInputSplit, Serializable>) this.format).getCurrentState();
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
	}

	//	---------------------			Checkpointing			--------------------------

	@Override
	public void snapshotState(FSDataOutputStream os, long checkpointId, long timestamp) throws Exception {
		final ObjectOutputStream oos = new ObjectOutputStream(os);

		List<TimestampedFileInputSplit> readerState = this.reader.getReaderState();
		oos.writeInt(readerState.size());
		for (TimestampedFileInputSplit split : readerState) {
			oos.writeObject(split);
		}
		oos.flush();
	}

	@Override
	public void restoreState(FSDataInputStream is) throws Exception {

		checkState(this.restoredReaderState == null,
			"The reader state has already been initialized.");

		final ObjectInputStream ois = new ObjectInputStream(is);

		int noOfSplits = ois.readInt();
		List<TimestampedFileInputSplit> pendingSplits = new ArrayList<>(noOfSplits);
		for (int i = 0; i < noOfSplits; i++) {
			pendingSplits.add((TimestampedFileInputSplit) ois.readObject());
		}
		this.restoredReaderState = pendingSplits;
	}
}
