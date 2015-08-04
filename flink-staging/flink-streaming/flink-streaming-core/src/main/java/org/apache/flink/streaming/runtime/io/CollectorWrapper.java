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

import org.apache.flink.streaming.api.collector.selector.OutputSelectorWrapper;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class CollectorWrapper<OUT> implements Output<StreamRecord<OUT>> {

	private OutputSelectorWrapper<OUT> outputSelectorWrapper;

	private List<Output<OUT>> allOutputs;

	public CollectorWrapper(OutputSelectorWrapper<OUT> outputSelectorWrapper) {
		this.outputSelectorWrapper = outputSelectorWrapper;
		allOutputs = new ArrayList<Output<OUT>>();
	}

	@SuppressWarnings("unchecked,rawtypes")
	public void addCollector(Output<StreamRecord<?>> output, StreamEdge edge) {
		outputSelectorWrapper.addCollector(output, edge);
		allOutputs.add((Output) output);
	}

	@Override
	public void collect(StreamRecord<OUT> record) {
		for (Collector<StreamRecord<OUT>> output : outputSelectorWrapper.getSelectedOutputs(record.getValue())) {
			output.collect(record);
		}
	}

	@Override
	public void emitWatermark(Watermark mark) {
		for (Output<OUT> output : allOutputs) {
			output.emitWatermark(mark);
		}
	}

	@Override
	public void close() {
	}

}
