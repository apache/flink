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

package org.apache.flink.table.runtime.operators.wmassigners;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.util.Preconditions;

/**
 * A stream operator that extracts timestamps from stream elements and
 * generates watermarks with specified emit latency.
 */
public class MiniBatchedWatermarkAssignerOperator
	extends AbstractStreamOperator<BaseRow>
	implements OneInputStreamOperator<BaseRow, BaseRow>, ProcessingTimeCallback {

	private final int rowtimeFieldIndex;

	private final long watermarkDelay;

	// timezone offset.
	private final long tzOffset;

	private final long idleTimeout;

	private long watermarkInterval;

	private transient long currentWatermark;

	private transient long expectedWatermark;

	private transient long lastRecordTime;

	private transient StreamStatusMaintainer streamStatusMaintainer;

	public MiniBatchedWatermarkAssignerOperator(
		int rowtimeFieldIndex,
		long watermarkDelay,
		long tzOffset,
		long idleTimeout,
		long watermarkInterval) {
		this.rowtimeFieldIndex = rowtimeFieldIndex;
		this.watermarkDelay = watermarkDelay;
		this.tzOffset = tzOffset;
		this.chainingStrategy = ChainingStrategy.ALWAYS;
		this.watermarkInterval = watermarkInterval;

		this.idleTimeout = idleTimeout;
	}

	@Override
	public void open() throws Exception {
		super.open();

		Preconditions.checkArgument(watermarkInterval > 0,
			"The inferred emit latency should be larger than 0");

		// timezone watermarkDelay should be considered when calculating watermark start time.
		currentWatermark = 0;
		expectedWatermark = getMiniBatchStart(currentWatermark, tzOffset, watermarkInterval)
			+ watermarkInterval - 1;

		if (idleTimeout > 0) {
			this.lastRecordTime = getProcessingTimeService().getCurrentProcessingTime();
			this.streamStatusMaintainer = getContainingTask().getStreamStatusMaintainer();
			getProcessingTimeService().registerTimer(lastRecordTime + idleTimeout, this);
		}
	}

	@Override
	public void processElement(StreamRecord<BaseRow> element) throws Exception {
		if (idleTimeout > 0) {
			// mark the channel active
			streamStatusMaintainer.toggleStreamStatus(StreamStatus.ACTIVE);
			lastRecordTime = getProcessingTimeService().getCurrentProcessingTime();
		}
		BaseRow row = element.getValue();
		if (row.isNullAt(rowtimeFieldIndex)) {
			throw new RuntimeException("RowTime field should not be null," +
				" please convert it to a non-null long value.");
		}
		long wm = row.getLong(rowtimeFieldIndex) - watermarkDelay;
		currentWatermark = Math.max(currentWatermark, wm);
		// forward element
		output.collect(element);

		if (currentWatermark >= expectedWatermark) {
			output.emitWatermark(new Watermark(currentWatermark));
			long start = getMiniBatchStart(currentWatermark, tzOffset, watermarkInterval);
			long end = start + watermarkInterval - 1;
			expectedWatermark = end > currentWatermark ? end : end + watermarkInterval;
		}
	}

	@Override
	public void onProcessingTime(long timestamp) throws Exception {
		if (idleTimeout > 0) {
			final long currentTime = getProcessingTimeService().getCurrentProcessingTime();
			if (currentTime - lastRecordTime > idleTimeout) {
				// mark the channel as idle to ignore watermarks from this channel
				streamStatusMaintainer.toggleStreamStatus(StreamStatus.IDLE);
			}
		}

		// register next timer
		long now = getProcessingTimeService().getCurrentProcessingTime();
		getProcessingTimeService().registerTimer(now + watermarkInterval, this);
	}

	/**
	 * Override the base implementation to completely ignore watermarks propagated from
	 * upstream (we rely only on the {@link MiniBatchedWatermarkAssignerOperator} to emit
	 * watermarks from here).
	 */
	@Override
	public void processWatermark(Watermark mark) throws Exception {
		// if we receive a Long.MAX_VALUE watermark we forward it since it is used
		// to signal the end of input and to not block watermark progress downstream
		if (mark.getTimestamp() == Long.MAX_VALUE && currentWatermark != Long.MAX_VALUE) {
			if (idleTimeout > 0) {
				// mark the channel active
				streamStatusMaintainer.toggleStreamStatus(StreamStatus.ACTIVE);
			}
			currentWatermark = Long.MAX_VALUE;
			output.emitWatermark(mark);
		}
	}

	public void endInput() throws Exception {
		processWatermark(Watermark.MAX_WATERMARK);
	}

	@Override
	public void close() throws Exception {
		endInput(); // TODO after introduce endInput
		super.close();
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------
	/**
	 * Method to get the mini-batch start for a watermark.
	 */
	public static long getMiniBatchStart(long watermark, long tzOffset, long interval) {
		return watermark - (watermark - tzOffset + interval) % interval;
	}
}
