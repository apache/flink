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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.operators.CountingOutput;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

/**
 * Wrapping {@link RecordWriterOutput} that updates metrics on the number of emitted elements.
 */
public class RecordWriterCountingOutput<OUT> extends CountingOutput<OUT> implements WatermarkGaugeExposingOutput<StreamRecord<OUT>> {
	private final RecordWriterOutput<OUT> output;

	public RecordWriterCountingOutput(Output<StreamRecord<OUT>> output, Counter counter) {
		super(output, counter);
		if (output instanceof RecordWriterOutput) {
			this.output = (RecordWriterOutput<OUT>) output;
		} else {
			throw new IllegalArgumentException("The type of output must be " + RecordWriterOutput.class);
		}
	}

	@Override
	public void emitWatermark(Watermark mark) {
		output.emitWatermark(mark);
	}

	@Override
	public void emitLatencyMarker(LatencyMarker latencyMarker) {
		output.emitLatencyMarker(latencyMarker);
	}

	@Override
	public void collect(StreamRecord<OUT> record) {
		if (output.collectAndCheckIfEmitted(record)) {
			numRecordsOut.inc();
		}
	}

	@Override
	public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
		if (output.collectAndCheckIfEmitted(outputTag, record)) {
			numRecordsOut.inc();
		}
	}

	@Override
	public void close() {
		output.close();
	}

	@Override
	public Gauge<Long> getWatermarkGauge() {
		return output.getWatermarkGauge();
	}
}
