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

package org.apache.flink.streaming.api.collector;

import org.apache.flink.streaming.api.collector.selector.OutputSelectorWrapper;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.util.Collector;

public class CollectorWrapper<OUT> implements Output<OUT> {

	private OutputSelectorWrapper<OUT> outputSelectorWrapper;

	public CollectorWrapper(OutputSelectorWrapper<OUT> outputSelectorWrapper) {
		this.outputSelectorWrapper = outputSelectorWrapper;
	}

	public void addCollector(Collector<?> output, StreamEdge edge) {
		outputSelectorWrapper.addCollector(output, edge);
	}

	@Override
	public void collect(OUT record) {
		for (Collector<OUT> output : outputSelectorWrapper.getSelectedOutputs(record)) {
			output.collect(record);
		}
	}

	@Override
	public void close() {
	}

}
