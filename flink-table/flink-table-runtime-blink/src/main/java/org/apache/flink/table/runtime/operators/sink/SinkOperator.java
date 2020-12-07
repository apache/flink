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

package org.apache.flink.table.runtime.operators.sink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;

/**
 * A {@link StreamOperator} for executing {@link SinkFunction SinkFunctions}. This operator
 * also checks writing null values into NOT NULL columns.
 */
public class SinkOperator extends AbstractUdfStreamOperator<Object, SinkFunction<RowData>>
	implements OneInputStreamOperator<RowData, Object> {

	private static final long serialVersionUID = 1L;

	private final int rowtimeFieldIndex;
	private final SinkNotNullEnforcer enforcer;

	private transient SimpleContext sinkContext;

	/** We listen to this ourselves because we don't have an {@link InternalTimerService}. */
	private long currentWatermark = Long.MIN_VALUE;

	public SinkOperator(
			SinkFunction<RowData> sinkFunction,
			int rowtimeFieldIndex,
			SinkNotNullEnforcer enforcer) {
		super(sinkFunction);
		this.rowtimeFieldIndex = rowtimeFieldIndex;
		this.enforcer = enforcer;
		chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void open() throws Exception {
		super.open();
		this.sinkContext = new SimpleContext(getProcessingTimeService());
	}

	@Override
	public void processElement(StreamRecord<RowData> element) throws Exception {
		sinkContext.element = element;
		RowData row = element.getValue();
		if (enforcer.filter(row)) {
			userFunction.invoke(row, sinkContext);
		}
	}

	@Override
	protected void reportOrForwardLatencyMarker(LatencyMarker marker) {
		// all operators are tracking latencies
		this.latencyStats.reportLatency(marker);

		// sinks don't forward latency markers
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		super.processWatermark(mark);
		this.currentWatermark = mark.getTimestamp();
	}

	private class SimpleContext implements SinkFunction.Context {

		private StreamRecord<RowData> element;

		private final ProcessingTimeService processingTimeService;

		public SimpleContext(ProcessingTimeService processingTimeService) {
			this.processingTimeService = processingTimeService;
		}

		@Override
		public long currentProcessingTime() {
			return processingTimeService.getCurrentProcessingTime();
		}

		@Override
		public long currentWatermark() {
			return currentWatermark;
		}

		@Override
		public Long timestamp() {
			if (rowtimeFieldIndex >= 0) {
				TimestampData timestamp = element.getValue().getTimestamp(rowtimeFieldIndex, 3);
				if (timestamp != null) {
					return timestamp.getMillisecond();
				} else {
					return null;
				}
			}
			return null;
		}
	}
}
