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

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A StreamCollector that uses user defined output names and a user defined
 * output selector to make directed emits.
 * 
 * @param <OUT>
 *            Type of the Tuple collected.
 */
public class DirectedOutputWrapper<OUT> extends StreamOutputWrapper<OUT> {

	private static final Logger LOG = LoggerFactory.getLogger(DirectedOutputWrapper.class);

	OutputSelector<OUT> outputSelector;

	protected Map<String, List<StreamOutput<OUT>>> outputMap;

	private List<StreamOutput<OUT>> selectAllOutputs;
	private Set<StreamOutput<OUT>> emitted;

	/**
	 * Creates a new DirectedStreamCollector
	 * 
	 * @param channelID
	 *            Channel ID of the Task
	 * @param serializationDelegate
	 *            Serialization delegate used for serialization
	 * @param outputSelector
	 *            User defined {@link OutputSelector}
	 */
	public DirectedOutputWrapper(int channelID,
			SerializationDelegate<StreamRecord<OUT>> serializationDelegate,
			OutputSelector<OUT> outputSelector) {
		super(channelID, serializationDelegate);
		this.outputSelector = outputSelector;
		this.emitted = new HashSet<StreamOutput<OUT>>();
		this.selectAllOutputs = new LinkedList<StreamOutput<OUT>>();
		this.outputMap = new HashMap<String, List<StreamOutput<OUT>>>();

	}

	@Override
	public void addOutput(StreamOutput<OUT> output) {

		super.addOutput(output);

		if (output.isSelectAll()) {
			selectAllOutputs.add(output);
		} else {
			for (String selectedName : output.getSelectedNames()) {
				if (selectedName != null) {
					if (!outputMap.containsKey(selectedName)) {
						outputMap.put(selectedName, new LinkedList<StreamOutput<OUT>>());
						outputMap.get(selectedName).add(output);
					} else {
						if (!outputMap.get(selectedName).contains(output)) {
							outputMap.get(selectedName).add(output);
						}
					}

				}
			}
		}
	}

	@Override
	protected void emit() {
		Iterable<String> outputNames = outputSelector.select(serializationDelegate.getInstance()
				.getObject());
		emitted.clear();

		for (StreamOutput<OUT> output : selectAllOutputs) {
			try {
				output.collect(serializationDelegate);
			} catch (Exception e) {
				if (LOG.isErrorEnabled()) {
					LOG.error("Emit to {} failed due to: {}", output,
							StringUtils.stringifyException(e));
				}
			}
		}

		for (String outputName : outputNames) {
			List<StreamOutput<OUT>> outputList = outputMap.get(outputName);
			try {
				if (outputList == null) {
					if (LOG.isErrorEnabled()) {
						String format = String.format(
								"Cannot emit because no output is selected with the name: %s",
								outputName);
						LOG.error(format);

					}
				} else {

					for (StreamOutput<OUT> output : outputList) {
						if (!emitted.contains(output)) {
							output.collect(serializationDelegate);
							emitted.add(output);
						}
					}

				}

			} catch (Exception e) {
				if (LOG.isErrorEnabled()) {
					LOG.error("Emit to {} failed due to: {}", outputName,
							StringUtils.stringifyException(e));
				}
			}

		}
	}
}
