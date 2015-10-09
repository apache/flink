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

package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * A {@link org.apache.flink.streaming.api.operators.StreamOperator} for extracting timestamps
 * from user elements and assigning them as the internal timestamp of the {@link StreamRecord}.
 *
 * @param <T> The type of the input elements
 */
public class ExtractTimestampsOperator<T>
		extends AbstractUdfStreamOperator<T, TimestampExtractor<T>>
		implements OneInputStreamOperator<T, T>, Triggerable {

	private static final long serialVersionUID = 1L;

	transient long watermarkInterval;

	transient long currentWatermark;

	public ExtractTimestampsOperator(TimestampExtractor<T> extractor) {
		super(extractor);
		chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		watermarkInterval = getRuntimeContext().getExecutionConfig().getAutoWatermarkInterval();
		if (watermarkInterval > 0) {
			getRuntimeContext().registerTimer(System.currentTimeMillis() + watermarkInterval, this);
		}

		currentWatermark = Long.MIN_VALUE;
	}

	@Override
	public void close() throws Exception {
		super.close();

		// emit a final +Inf watermark, just like the sources
		output.emitWatermark(new Watermark(Long.MAX_VALUE));
	}

	@Override
	public void processElement(StreamRecord<T> element) throws Exception {
		long newTimestamp = userFunction.extractTimestamp(element.getValue(), element.getTimestamp());
		output.collect(element.replace(element.getValue(), newTimestamp));
		long watermark = userFunction.extractWatermark(element.getValue(), newTimestamp);
		if (watermark > currentWatermark) {
			currentWatermark = watermark;
			output.emitWatermark(new Watermark(currentWatermark));
		}
	}

	@Override
	public void trigger(long timestamp) throws Exception {
		// register next timer
		getRuntimeContext().registerTimer(System.currentTimeMillis() + watermarkInterval, this);
		long lastWatermark = currentWatermark;
		currentWatermark = userFunction.getCurrentWatermark();

		if (currentWatermark > lastWatermark) {
			// emit watermark
			output.emitWatermark(new Watermark(currentWatermark));
		}
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		// ingore them, since we are basically a watermark source
	}
}
