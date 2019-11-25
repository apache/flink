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
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OutputTypeConfigurable;
import org.apache.flink.streaming.api.operators.StreamSourceContexts;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.PriorityQueue;

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
	implements OneInputStreamOperator<TimestampedFileInputSplit, OUT>, OutputTypeConfigurable<OUT> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(ContinuousFileReaderOperator.class);

	private FileInputFormat<OUT> format;
	private Counter completedSplitsCounter;
	private TypeSerializer<OUT> serializer;

	private transient SourceFunction.SourceContext<OUT> readerContext;

	private transient ListState<TimestampedFileInputSplit> checkpointedState;
	private PriorityQueue<TimestampedFileInputSplit> splits;
	private Object checkpointLock;

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

		checkState(checkpointedState == null, "The reader state has already been initialized.");

		checkpointedState = context.getOperatorStateStore().getSerializableListState("splits");

		int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();
		if (!context.isRestored()) {
			LOG.info("No state to restore for the {} (taskIdx={}).", getClass().getSimpleName(), subtaskIdx);
			splits = splits == null ? new PriorityQueue<>() : splits;
			return;
		}

		LOG.info("Restoring state for the {} (taskIdx={}).", getClass().getSimpleName(), subtaskIdx);

		// this may not be null in case we migrate from a previous Flink version.
		if (splits != null) {
			return;
		}

		splits = new PriorityQueue<>();
		for (TimestampedFileInputSplit split : checkpointedState.get()) {
			splits.add(split);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("{} (taskIdx={}) restored {}.", getClass().getSimpleName(), subtaskIdx, splits);
		}
	}

	@Override
	public void open() throws Exception {
		super.open();

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

		this.completedSplitsCounter = getMetricGroup().counter("numSplitsProcessed");
		this.format.openInputFormat();
		processSplits();
	}

	@Override
	public void processElement(StreamRecord<TimestampedFileInputSplit> element) throws Exception {
		splits.offer(element.getValue());
		processSplits();
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		// we do nothing because we emit our own watermarks if needed.
	}

	@Override
	public void dispose() throws Exception {
		super.dispose();

		readerContext = null;
		splits = null;
		format = null;
		serializer = null;
	}

	@Override
	public void close() throws Exception {
		super.close();

		if (readerContext != null) {
			readerContext.emitWatermark(Watermark.MAX_WATERMARK);
			readerContext.close();
			readerContext = null;
		}

		output.close();
	}

	private void processSplits() throws Exception {
		while (!splits.isEmpty()) {
			processSplit(splits.poll());
		}
	}

	private void processSplit(TimestampedFileInputSplit split) throws IOException {
		load(split);
		try {
			readSplit();

		} finally {
			synchronized (checkpointLock) {
				if (format != null) {
					this.format.close();
					this.format.closeInputFormat();
				}
			}
		}
	}

	private void load(TimestampedFileInputSplit split) throws IOException {
		if (this.format instanceof CheckpointableInputFormat && split.getSplitState() != null) {
			// recovering after a node failure with an input
			// format that supports resetting the offset
			synchronized (checkpointLock) {
				((CheckpointableInputFormat<TimestampedFileInputSplit, Serializable>) this.format).
						reopen(split, split.getSplitState());
			}
		} else {
			// we either have a new split, or we recovered from a node
			// failure but the input format does not support resetting the offset.
			synchronized (checkpointLock) {
				this.format.open(split);
			}
		}
	}

	private void readSplit() throws IOException {
		OUT nextElement = serializer.createInstance();
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
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		super.snapshotState(context);

		checkState(checkpointedState != null,
			"The operator state has not been properly initialized.");

		int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();

		checkpointedState.clear();

		try {
			for (TimestampedFileInputSplit split : splits) {
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
				getClass().getSimpleName(), subtaskIdx, splits.size(), splits);
		}
	}
}
