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

package org.apache.flink.streaming.api.collector.selector;

import java.util.ArrayList;

import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

public class BroadcastOutputSelectorWrapper<OUT> implements OutputSelectorWrapper<OUT> {

	private static final long serialVersionUID = 1L;
	
	private final ArrayList<Collector<StreamRecord<OUT>>> outputs;

	public BroadcastOutputSelectorWrapper() {
		outputs = new ArrayList<Collector<StreamRecord<OUT>>>();
	}
	
	@Override
	public void addCollector(Collector<StreamRecord<OUT>> output, StreamEdge edge) {
		outputs.add(output);
	}

	@Override
	public Iterable<Collector<StreamRecord<OUT>>> getSelectedOutputs(OUT record) {
		return outputs;
	}
}
