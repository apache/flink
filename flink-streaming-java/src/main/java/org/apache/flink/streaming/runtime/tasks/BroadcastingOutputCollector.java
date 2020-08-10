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
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusProvider;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.XORShiftRandom;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

class BroadcastingOutputCollector<T> implements WatermarkGaugeExposingOutput<StreamRecord<T>> {

	protected final Output<StreamRecord<T>>[] allOutputs;
	protected final Output<StreamRecord<T>>[] chainedOutputs;
	protected final RecordWriterOutput<T>[] nonChainedOutputs;

	protected final Counter numRecordsOutForTask;

	private final Random random = new XORShiftRandom();
	private final StreamStatusProvider streamStatusProvider;
	private final WatermarkGauge watermarkGauge = new WatermarkGauge();

	public BroadcastingOutputCollector(
			Output<StreamRecord<T>>[] allOutputs,
			StreamStatusProvider streamStatusProvider,
			Counter numRecordsOutForTask) {
		this.allOutputs = allOutputs;
		this.streamStatusProvider = streamStatusProvider;
		this.numRecordsOutForTask = numRecordsOutForTask;

		List<Output<StreamRecord<T>>> chainedOutputs = new ArrayList<>(4);
		List<RecordWriterOutput<T>> nonChainedOutputs = new ArrayList<>(4);
		for (Output<StreamRecord<T>> output : allOutputs) {
			if (output instanceof RecordWriterOutput) {
				nonChainedOutputs.add((RecordWriterOutput<T>) output);
			} else {
				chainedOutputs.add(output);
			}
		}
		this.chainedOutputs = chainedOutputs.toArray(new Output[chainedOutputs.size()]);
		this.nonChainedOutputs = nonChainedOutputs.toArray(new RecordWriterOutput[nonChainedOutputs.size()]);
	}

	@Override
	public void emitWatermark(Watermark mark) {
		watermarkGauge.setCurrentWatermark(mark.getTimestamp());
		if (streamStatusProvider.getStreamStatus().isActive()) {
			for (Output<StreamRecord<T>> output : allOutputs) {
				output.emitWatermark(mark);
			}
		}
	}

	@Override
	public void emitLatencyMarker(LatencyMarker latencyMarker) {
		if (allOutputs.length <= 0) {
			// ignore
		} else if (allOutputs.length == 1) {
			allOutputs[0].emitLatencyMarker(latencyMarker);
		} else {
			// randomly select an output
			allOutputs[random.nextInt(allOutputs.length)].emitLatencyMarker(latencyMarker);
		}
	}

	@Override
	public Gauge<Long> getWatermarkGauge() {
		return watermarkGauge;
	}

	@Override
	public void collect(StreamRecord<T> record) {
		boolean emitted = false;
		for (RecordWriterOutput<T> output : nonChainedOutputs) {
			emitted |= output.collectAndCheckIfEmitted(record);
		}

		for (Output<StreamRecord<T>> output : chainedOutputs) {
			output.collect(record);
		}

		if (emitted) {
			numRecordsOutForTask.inc();
		}
	}

	@Override
	public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
		boolean emitted = false;
		for (RecordWriterOutput<T> output : nonChainedOutputs) {
			emitted |= output.collectAndCheckIfEmitted(outputTag, record);
		}

		for (Output<StreamRecord<T>> output : chainedOutputs) {
			output.collect(outputTag, record);
		}
		if (emitted) {
			numRecordsOutForTask.inc();
		}
	}

	@Override
	public void close() {
		for (Output<StreamRecord<T>> output : allOutputs) {
			output.close();
		}
	}
}
