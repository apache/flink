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

import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusSubMaintainer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The type Source input processor.
 */
class SourceInputProcessor implements InputProcessor, StatusWatermarkValve.ValveOutputHandler {

	private final OneInputStreamOperator sourceOperatorProxy;

	private final StatusWatermarkValve statusWatermarkValve;

	private final StreamStatusSubMaintainer streamStatusSubMaintainer;

	/**
	 * Instantiates a new Source input processor.
	 *
	 * @param streamStatusSubMaintainer the stream status sub maintainer
	 * @param sourceOperatorProxy       the source operator proxy
	 * @param checkpointLock            the checkpoint lock
	 * @param taskMetricGroup           the task metric group
	 * @param channelCount              the channel count
	 */
	public SourceInputProcessor(
		StreamStatusSubMaintainer streamStatusSubMaintainer,
		OneInputStreamOperator sourceOperatorProxy,
		Object checkpointLock,
		TaskMetricGroup taskMetricGroup,
		int channelCount) {

		this.streamStatusSubMaintainer = checkNotNull(streamStatusSubMaintainer);

		this.statusWatermarkValve = new StatusWatermarkValve(channelCount, this);

		this.sourceOperatorProxy = checkNotNull(sourceOperatorProxy);
	}

	@Override
	public void processRecord(StreamRecord streamRecord, int channelIndex) throws Exception {
		throw new UnsupportedOperationException("SourceInputProcessor should not process record");
	}

	@Override
	public void processLatencyMarker(LatencyMarker latencyMarker, int channelIndex) throws Exception {
		throw new UnsupportedOperationException("SourceInputProcessor should not process latency marker");
	}

	@Override
	public void processWatermark(Watermark watermark, int channelIndex) throws Exception {
		throw new UnsupportedOperationException("SourceInputProcessor should not process watermark");
	}

	@Override
	public void processStreamStatus(StreamStatus streamStatus, int channelIndex) throws Exception {
		throw new UnsupportedOperationException("SourceInputProcessor should not process stream status");
	}

	@Override
	public void endInput() throws Exception {
		sourceOperatorProxy.endInput();
	}

	@Override
	public void handleWatermark(Watermark watermark) {
		throw new UnsupportedOperationException("SourceInputProcessor should not process watermark");
	}

	@Override
	public void handleStreamStatus(StreamStatus streamStatus) {
		streamStatusSubMaintainer.updateStreamStatus(streamStatus);
	}

	@Override
	public void release() {
		streamStatusSubMaintainer.release();
	}
}

