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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectedOutputSelectorWrapper<OUT> implements OutputSelectorWrapper<OUT> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(DirectedOutputSelectorWrapper.class);

	private List<OutputSelector<OUT>> outputSelectors;

	private HashMap<String, ArrayList<Collector<StreamRecord<OUT>>>> outputMap;
	private HashSet<Collector<StreamRecord<OUT>>> selectAllOutputs;

	public DirectedOutputSelectorWrapper(List<OutputSelector<OUT>> outputSelectors) {
		this.outputSelectors = outputSelectors;
		this.selectAllOutputs = new HashSet<Collector<StreamRecord<OUT>>>();
		this.outputMap = new HashMap<String, ArrayList<Collector<StreamRecord<OUT>>>>();
	}
	
	@Override
	public void addCollector(Collector<StreamRecord<OUT>> output, StreamEdge edge) {
		List<String> selectedNames = edge.getSelectedNames();

		if (selectedNames.isEmpty()) {
			selectAllOutputs.add(output);
		}
		else {
			for (String selectedName : selectedNames) {
				if (!outputMap.containsKey(selectedName)) {
					outputMap.put(selectedName, new ArrayList<Collector<StreamRecord<OUT>>>());
					outputMap.get(selectedName).add(output);
				}
				else {
					if (!outputMap.get(selectedName).contains(output)) {
						outputMap.get(selectedName).add(output);
					}
				}
			}
		}
	}

	@Override
	public Iterable<Collector<StreamRecord<OUT>>> getSelectedOutputs(OUT record) {
		Set<Collector<StreamRecord<OUT>>> selectedOutputs = new HashSet<Collector<StreamRecord<OUT>>>(selectAllOutputs);

		for (OutputSelector<OUT> outputSelector : outputSelectors) {
			Iterable<String> outputNames = outputSelector.select(record);

			for (String outputName : outputNames) {
				List<Collector<StreamRecord<OUT>>> outputList = outputMap.get(outputName);

				try {
					selectedOutputs.addAll(outputList);
				} catch (NullPointerException e) {
					if (LOG.isErrorEnabled()) {
						String format = String.format(
								"Cannot emit because no output is selected with the name: %s",
								outputName);
						LOG.error(format);
					}
				}
			}
		}

		return selectedOutputs;
	}
}
