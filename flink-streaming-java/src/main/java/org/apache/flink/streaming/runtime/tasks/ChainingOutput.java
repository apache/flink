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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusProvider;
import org.apache.flink.util.OutputTag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

class ChainingOutput<T> implements WatermarkGaugeExposingOutput<StreamRecord<T>> {
	private static final Logger LOG = LoggerFactory.getLogger(ChainingOutput.class);

	protected final Input<T> input;
	protected final Counter numRecordsIn;
	protected final WatermarkGauge watermarkGauge = new WatermarkGauge();
	protected final StreamStatusProvider streamStatusProvider;
	@Nullable protected final OutputTag<T> outputTag;
	@Nullable protected final AutoCloseable closeable;

	public ChainingOutput(
			OneInputStreamOperator<T, ?> operator,
			StreamStatusProvider streamStatusProvider,
			@Nullable OutputTag<T> outputTag) {
		this(
			operator,
			(OperatorMetricGroup) operator.getMetricGroup(),
			streamStatusProvider,
			outputTag,
			operator::close);
	}

	public ChainingOutput(
			Input<T> input,
			OperatorMetricGroup operatorMetricGroup,
			StreamStatusProvider streamStatusProvider,
			@Nullable OutputTag<T> outputTag,
			@Nullable AutoCloseable closeable) {
		this.input = input;
		this.closeable = closeable;

		{
			Counter tmpNumRecordsIn;
			try {
				OperatorIOMetricGroup ioMetricGroup = operatorMetricGroup.getIOMetricGroup();
				tmpNumRecordsIn = ioMetricGroup.getNumRecordsInCounter();
			} catch (Exception e) {
				LOG.warn("An exception occurred during the metrics setup.", e);
				tmpNumRecordsIn = new SimpleCounter();
			}
			numRecordsIn = tmpNumRecordsIn;
		}

		this.streamStatusProvider = streamStatusProvider;
		this.outputTag = outputTag;
	}

	@Override
	public void collect(StreamRecord<T> record) {
		if (this.outputTag != null) {
			// we are not responsible for emitting to the main output.
			return;
		}

		pushToOperator(record);
	}

	@Override
	public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
		if (OutputTag.isResponsibleFor(this.outputTag, outputTag)) {
			pushToOperator(record);
		}
	}

	protected <X> void pushToOperator(StreamRecord<X> record) {
		try {
			// we know that the given outputTag matches our OutputTag so the record
			// must be of the type that our operator expects.
			@SuppressWarnings("unchecked")
			StreamRecord<T> castRecord = (StreamRecord<T>) record;

			numRecordsIn.inc();
			input.setKeyContextElement(castRecord);
			input.processElement(castRecord);
		}
		catch (Exception e) {
			throw new ExceptionInChainedOperatorException(e);
		}
	}

	@Override
	public void emitWatermark(Watermark mark) {
		try {
			watermarkGauge.setCurrentWatermark(mark.getTimestamp());
			if (streamStatusProvider.getStreamStatus().isActive()) {
				input.processWatermark(mark);
			}
		}
		catch (Exception e) {
			throw new ExceptionInChainedOperatorException(e);
		}
	}

	@Override
	public void emitLatencyMarker(LatencyMarker latencyMarker) {
		try {
			input.processLatencyMarker(latencyMarker);
		}
		catch (Exception e) {
			throw new ExceptionInChainedOperatorException(e);
		}
	}

	@Override
	public void close() {
		try {
			if (closeable != null) {
				closeable.close();
			}
		}
		catch (Exception e) {
			throw new ExceptionInChainedOperatorException(e);
		}
	}

	@Override
	public Gauge<Long> getWatermarkGauge() {
		return watermarkGauge;
	}
}
