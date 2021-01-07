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

package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * A special version of the per-pulsar-partition-state that additionally holds a {@link
 * TimestampAssigner}, {@link WatermarkGenerator}, an immediate {@link WatermarkOutput}, and a
 * deferred {@link WatermarkOutput} for this partition.
 *
 * <p>See {@link org.apache.flink.api.common.eventtime.WatermarkOutputMultiplexer} for an
 * explanation
 * of immediate and deferred {@link WatermarkOutput WatermarkOutputs.}.
 *
 * @param <T> The type of records handled by the watermark generator
 */
@Internal
public final class PulsarTopicPartitionStateWithWatermarkGenerator<T> extends PulsarTopicState<T> {

	private final TimestampAssigner<T> timestampAssigner;

	private final WatermarkGenerator<T> watermarkGenerator;

	/**
	 * Refer to {@link org.apache.flink.api.common.eventtime.WatermarkOutputMultiplexer} for
	 * a description of immediate/deferred output.
	 */
	private final WatermarkOutput immediateOutput;

	/**
	 * Refer to {@link org.apache.flink.api.common.eventtime.WatermarkOutputMultiplexer} for
	 * a description of immediate/deferred output.
	 */
	private final WatermarkOutput deferredOutput;

	// ------------------------------------------------------------------------

	public PulsarTopicPartitionStateWithWatermarkGenerator(
		TopicRange topicRange,
		PulsarTopicState<T> topicState,
		TimestampAssigner<T> timestampAssigner,
		WatermarkGenerator<T> watermarkGenerator,
		WatermarkOutput immediateOutput,
		WatermarkOutput deferredOutput) {
		super(topicRange);

		this.timestampAssigner = timestampAssigner;
		this.watermarkGenerator = watermarkGenerator;
		this.immediateOutput = immediateOutput;
		this.deferredOutput = deferredOutput;
	}

	// ------------------------------------------------------------------------

	@Override
	public long extractTimestamp(T record, long pulsarEventTimestamp) {
		return timestampAssigner.extractTimestamp(record, pulsarEventTimestamp);
	}

	@Override
	public void onEvent(T event, long timestamp) {
		watermarkGenerator.onEvent(event, timestamp, immediateOutput);
	}

	@Override
	public void onPeriodicEmit() {
		watermarkGenerator.onPeriodicEmit(deferredOutput);
	}

	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "PulsarTopicPartitionStateWithPeriodicWatermarks: partition=" + getTopicRange()
			+ ", offset=" + getOffset();
	}
}
