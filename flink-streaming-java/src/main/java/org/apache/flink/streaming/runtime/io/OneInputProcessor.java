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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.metrics.MinWatermarkGauge;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusSubMaintainer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The type One input processor.
 */
class OneInputProcessor implements InputProcessor, StatusWatermarkValve.ValveOutputHandler {

	private Counter numRecordsIn;

	private final OneInputStreamOperator operator;

	private final WatermarkGauge watermarkGauge = new WatermarkGauge();

	private final StatusWatermarkValve statusWatermarkValve;

	private final Object checkpointLock;

	private final TaskMetricGroup taskMetricGroup;

	private final StreamStatusSubMaintainer streamStatusSubMaintainer;

	/**
	 * Instantiates a new One input processor.
	 *
	 * @param streamStatusSubMaintainer the stream status sub maintainer
	 * @param operator                  the operator
	 * @param checkpointLock            the checkpoint lock
	 * @param taskMetricGroup           the task metric group
	 * @param minAllInputWatermarkGauge the min all input watermark gauge
	 * @param channelCount              the channel count
	 */
	public OneInputProcessor(
		StreamStatusSubMaintainer streamStatusSubMaintainer,
		OneInputStreamOperator operator,
		Object checkpointLock,
		TaskMetricGroup taskMetricGroup,
		MinWatermarkGauge minAllInputWatermarkGauge,
		int channelCount) {

		this.streamStatusSubMaintainer = streamStatusSubMaintainer;
		this.checkpointLock = checkNotNull(checkpointLock);
		this.taskMetricGroup = checkNotNull(taskMetricGroup);
		this.statusWatermarkValve = new StatusWatermarkValve(channelCount, this);
		this.operator = checkNotNull(operator);

		operator.getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, this.watermarkGauge);
		minAllInputWatermarkGauge.addWatermarkGauge(watermarkGauge);

		numRecordsIn = ((OperatorMetricGroup) operator.getMetricGroup()).getIOMetricGroup().getNumRecordsInCounter();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void processRecord(StreamRecord streamRecord, int channelIndex) throws Exception {
		synchronized (checkpointLock) {
			numRecordsIn.inc();
			operator.setKeyContextElement1(streamRecord);
			operator.processElement(streamRecord);
		}
	}

	@Override
	public void processLatencyMarker(LatencyMarker latencyMarker, int channelIndex) throws Exception {
		synchronized (checkpointLock) {
			operator.processLatencyMarker(latencyMarker);
		}
	}

	@Override
	public void processWatermark(Watermark watermark, int channelIndex) throws Exception {
		statusWatermarkValve.inputWatermark(watermark, channelIndex);
	}

	@Override
	public void processStreamStatus(StreamStatus streamStatus, int channelIndex) throws Exception {
		statusWatermarkValve.inputStreamStatus(streamStatus, channelIndex);
	}

	@Override
	public void endInput() throws Exception {
		operator.endInput();
	}

	@Override
	public void handleWatermark(Watermark watermark) {
		try {
			watermarkGauge.setCurrentWatermark(watermark.getTimestamp());
			operator.processWatermark(watermark);
		} catch (Exception e) {
			throw new RuntimeException("Exception occurred while processing valve output watermark: ", e);
		}
	}

	@Override
	public void handleStreamStatus(StreamStatus streamStatus) {
		streamStatusSubMaintainer.updateStreamStatus(streamStatus);
	}

	@Override
	public void release() {
		streamStatusSubMaintainer.release();
	}

	@VisibleForTesting
	WatermarkGauge getWatermarkGauge() {
		return watermarkGauge;
	}
}


