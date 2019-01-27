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

package org.apache.flink.table.runtime;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.table.dataformat.BaseRow;

/**
 * A stream operator that extracts timestamps from stream elements and
 * generates periodic watermarks.
 */
public class WatermarkAssignerOperator
	extends AbstractStreamOperator<BaseRow>
	implements OneInputStreamOperator<BaseRow, BaseRow>, ProcessingTimeCallback {

	private static final long serialVersionUID = 1L;

	private final int rowtimeIndex;

	private final long offset;

	private transient long watermarkInterval;

	private transient long currentWatermark;

	private transient long currentMaxTimestamp;

	public WatermarkAssignerOperator(int rowtimeIndex, long offset) {
		this.rowtimeIndex = rowtimeIndex;
		this.offset = offset;
		this.chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void open() throws Exception {
		super.open();

		// watermark and timestamp should start from 0
		currentWatermark = 0;
		currentMaxTimestamp = 0;
		watermarkInterval = getExecutionConfig().getAutoWatermarkInterval();

		if (watermarkInterval > 0) {
			long now = getProcessingTimeService().getCurrentProcessingTime();
			getProcessingTimeService().registerTimer(now + watermarkInterval, this);
		}
	}

	@Override
	public void processElement(StreamRecord<BaseRow> element) throws Exception {
		BaseRow row = element.getValue();
		if (row.isNullAt(rowtimeIndex)) {
			throw new RuntimeException("RowTime field should not be null," +
					" please convert it to a non-null long value.");
		}
		long ts = row.getLong(rowtimeIndex);
		currentMaxTimestamp = Math.max(currentMaxTimestamp, ts);
		// forward element
		output.collect(element);

		// eagerly emit watermark to avoid period timer not called
		// current_ts - last_ts > interval
		if (currentMaxTimestamp - (currentWatermark + offset) > watermarkInterval) {
			advanceWatermark();
		}
	}

	private void advanceWatermark() {
		long newWatermark = currentMaxTimestamp - offset;
		if (newWatermark > currentWatermark) {
			currentWatermark = newWatermark;
			// emit watermark
			output.emitWatermark(new Watermark(newWatermark));
		}
	}

	@Override
	public void onProcessingTime(long timestamp) throws Exception {
		advanceWatermark();

		// register next timer
		long now = getProcessingTimeService().getCurrentProcessingTime();
		getProcessingTimeService().registerTimer(now + watermarkInterval, this);
	}

	/**
	 * Override the base implementation to completely ignore watermarks propagated from
	 * upstream (we rely only on the {@link AssignerWithPeriodicWatermarks} to emit
	 * watermarks from here).
	 */
	@Override
	public void processWatermark(Watermark mark) throws Exception {
		// if we receive a Long.MAX_VALUE watermark we forward it since it is used
		// to signal the end of input and to not block watermark progress downstream
		if (mark.getTimestamp() == Long.MAX_VALUE && currentWatermark != Long.MAX_VALUE) {
			currentWatermark = Long.MAX_VALUE;
			output.emitWatermark(mark);
		}
	}

	@Override
	public void endInput() throws Exception {
		processWatermark(Watermark.MAX_WATERMARK);
	}

	@Override
	public void close() throws Exception {
		super.close();

		// emit a final watermark
		advanceWatermark();
	}
}
