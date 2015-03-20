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
import java.util.List;

import org.apache.flink.streaming.api.StreamEdge;
import org.apache.flink.util.Collector;

public class BroadcastOutputSelectorWrapper<OUT> implements OutputSelectorWrapper<OUT> {

	private static final long serialVersionUID = 1L;
	private List<Collector<OUT>> outputs;

	public BroadcastOutputSelectorWrapper() {
		outputs = new ArrayList<Collector<OUT>>();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void addCollector(Collector<?> output, StreamEdge edge) {
		outputs.add((Collector<OUT>) output);
	}

	@Override
	public Iterable<Collector<OUT>> getSelectedOutputs(OUT record) {
		return outputs;
	}
}
