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

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.flink.runtime.io.network.api.RecordWriter;
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
public class DirectedStreamCollector<OUT> extends StreamCollector<OUT> {

	private static final Logger LOG = LoggerFactory.getLogger(DirectedStreamCollector.class);

	OutputSelector<OUT> outputSelector;
	private List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> selectAllOutputs;
	private Set<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> emitted;

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
	public DirectedStreamCollector(int channelID,
			SerializationDelegate<StreamRecord<OUT>> serializationDelegate,
			OutputSelector<OUT> outputSelector) {
		super(channelID, serializationDelegate);
		this.outputSelector = outputSelector;
		this.emitted = new HashSet<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>>();
		this.selectAllOutputs = new LinkedList<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>>();
	}

	@Override
	public void addOutput(RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output,
			List<String> outputNames, boolean isSelectAllOutput) {

		if (isSelectAllOutput) {
			selectAllOutputs.add(output);
		} else {
			addOneOutput(output, outputNames, isSelectAllOutput);
		}
	}

	/**
	 * Emits a StreamRecord to the outputs selected by the user defined
	 * OutputSelector
	 *
	 */
	protected void emitToOutputs() {
		Iterable<String> outputNames = outputSelector.select(streamRecord.getObject());
		emitted.clear();

		for (RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output : selectAllOutputs) {
			try {
				output.emit(serializationDelegate);
			} catch (Exception e) {
				if (LOG.isErrorEnabled()) {
					LOG.error("Emit to {} failed due to: {}", output,
							StringUtils.stringifyException(e));
				}
			}
		}
		emitted.addAll(selectAllOutputs);

		for (String outputName : outputNames) {
			List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> outputList = outputMap
					.get(outputName);
			try {
				if (outputList == null) {
					if (LOG.isErrorEnabled()) {
						String format = String.format(
								"Cannot emit because no output is selected with the name: %s",
								outputName);
						LOG.error(format);

					}
				} else {

					for (RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output : outputList) {
						if (!emitted.contains(output)) {
							output.emit(serializationDelegate);
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
