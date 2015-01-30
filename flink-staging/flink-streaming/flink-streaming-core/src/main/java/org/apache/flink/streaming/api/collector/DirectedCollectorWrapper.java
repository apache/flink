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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A StreamCollector that uses user defined output names and a user defined
 * output selector to make directed emits.
 * 
 * @param <OUT>
 *            Type of the Tuple collected.
 */
public class DirectedCollectorWrapper<OUT> extends CollectorWrapper<OUT> {

	private static final Logger LOG = LoggerFactory.getLogger(DirectedCollectorWrapper.class);

	List<OutputSelector<OUT>> outputSelectors;

	protected Map<String, List<Collector<OUT>>> outputMap;

	private List<Collector<OUT>> selectAllOutputs;
	private Set<Collector<OUT>> emitted;

	/**
	 * Creates a new DirectedStreamCollector
	 * 
	 * @param outputSelector
	 *            User defined {@link OutputSelector}
	 */
	public DirectedCollectorWrapper(List<OutputSelector<OUT>> outputSelectors) {
		this.outputSelectors = outputSelectors;
		this.emitted = new HashSet<Collector<OUT>>();
		this.selectAllOutputs = new LinkedList<Collector<OUT>>();
		this.outputMap = new HashMap<String, List<Collector<OUT>>>();

	}

	@Override
	public void addCollector(Collector<?> output) {
		addCollector(output, new ArrayList<String>());
	}

	@SuppressWarnings("unchecked")
	public void addCollector(Collector<?> output, List<String> selectedNames) {

		if (selectedNames.isEmpty()) {
			selectAllOutputs.add((Collector<OUT>) output);
		} else {
			for (String selectedName : selectedNames) {

				if (!outputMap.containsKey(selectedName)) {
					outputMap.put(selectedName, new LinkedList<Collector<OUT>>());
					outputMap.get(selectedName).add((Collector<OUT>) output);
				} else {
					if (!outputMap.get(selectedName).contains(output)) {
						outputMap.get(selectedName).add((Collector<OUT>) output);
					}
				}

			}
		}
	}

	@Override
	public void collect(OUT record) {
		emitted.clear();

		for (Collector<OUT> output : selectAllOutputs) {
			output.collect(record);
			emitted.add(output);
		}

		for (OutputSelector<OUT> outputSelector : outputSelectors) {
			Iterable<String> outputNames = outputSelector.select(record);

			for (String outputName : outputNames) {
				List<Collector<OUT>> outputList = outputMap.get(outputName);
				if (outputList == null) {
					if (LOG.isErrorEnabled()) {
						String format = String.format(
								"Cannot emit because no output is selected with the name: %s",
								outputName);
						LOG.error(format);

					}
				} else {
					for (Collector<OUT> output : outputList) {
						if (!emitted.contains(output)) {
							output.collect(record);
							emitted.add(output);
						}
					}

				}

			}
		}

	}

	@Override
	public void close() {

	}
}
