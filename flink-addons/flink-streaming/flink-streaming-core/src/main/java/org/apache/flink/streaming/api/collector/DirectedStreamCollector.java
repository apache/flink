/**
 *
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
 *
 */

package org.apache.flink.streaming.api.collector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.runtime.io.network.api.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.util.StringUtils;

/**
 * A StreamCollector that uses user defined output names and a user defined
 * output selector to make directed emits.
 * 
 * @param <OUT>
 *            Type of the Tuple collected.
 */
public class DirectedStreamCollector<OUT> extends StreamCollector<OUT> {

	OutputSelector<OUT> outputSelector;
	private static final Log LOG = LogFactory.getLog(DirectedStreamCollector.class);
	private List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> emitted;

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
		this.emitted = new ArrayList<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>>();

	}

	/**
	 * Collects and emits a tuple to the outputs by reusing a StreamRecord
	 * object.
	 * 
	 * @param outputObject
	 *            Object to be collected and emitted.
	 */
	@Override
	public void collect(OUT outputObject) {
		streamRecord.setObject(outputObject);
		emit(streamRecord);
	}

	/**
	 * Emits a StreamRecord to the outputs selected by the user defined
	 * OutputSelector
	 * 
	 * @param streamRecord
	 *            Record to emit.
	 */
	private void emit(StreamRecord<OUT> streamRecord) {
		Collection<String> outputNames = outputSelector.getOutputs(streamRecord.getObject());
		streamRecord.setId(channelID);
		serializationDelegate.setInstance(streamRecord);
		emitted.clear();
		for (String outputName : outputNames) {
			try {
				for (RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output : outputMap
						.get(outputName)) {
					if (!emitted.contains(output)) {
						output.emit(serializationDelegate);
						emitted.add(output);
					}
				}
			} catch (Exception e) {
				if (LOG.isErrorEnabled()) {
					LOG.error(String.format("Emit to %s failed due to: %s", outputName,
							StringUtils.stringifyException(e)));
				}
			}
		}
	}
}
