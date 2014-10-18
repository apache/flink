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
import java.util.List;
import java.util.Map;

import org.apache.flink.runtime.io.network.api.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collector for tuples in Apache Flink stream processing. The collected values
 * will be wrapped with ID in a {@link StreamRecord} and then emitted to the
 * outputs.
 * 
 * @param <OUT>
 *            Type of the Tuples/Objects collected.
 */
public class StreamCollector<OUT> implements Collector<OUT> {

	private static final Logger LOG = LoggerFactory.getLogger(StreamCollector.class);

	protected StreamRecord<OUT> streamRecord;
	protected int channelID;
	protected List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> outputs;
	protected Map<String, List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>>> outputMap;
	protected SerializationDelegate<StreamRecord<OUT>> serializationDelegate;

	/**
	 * Creates a new StreamCollector
	 * 
	 * @param channelID
	 *            Channel ID of the Task
	 * @param serializationDelegate
	 *            Serialization delegate used for serialization
	 */
	public StreamCollector(int channelID,
			SerializationDelegate<StreamRecord<OUT>> serializationDelegate) {
		this.serializationDelegate = serializationDelegate;

		if (serializationDelegate != null) {
			this.streamRecord = serializationDelegate.getInstance();
		} else {
			this.streamRecord = new StreamRecord<OUT>();
		}
		this.channelID = channelID;
		this.outputs = new ArrayList<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>>();
		this.outputMap = new HashMap<String, List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>>>();
	}

	/**
	 * Adds an output with the given user defined name
	 * 
	 * @param output
	 *            The RecordWriter object representing the output.
	 * @param outputNames
	 *            User defined names of the output.
	 * @param isSelectAllOutput
	 *            Marks whether all the outputs are selected.
	 */
	public void addOutput(RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output,
			List<String> outputNames, boolean isSelectAllOutput) {
		addOneOutput(output, outputNames, isSelectAllOutput);
	}

	protected void addOneOutput(RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output,
			List<String> outputNames, boolean isSelectAllOutput) {
		outputs.add(output);
		for (String outputName : outputNames) {
			if (outputName != null) {
				if (!outputMap.containsKey(outputName)) {
					outputMap
							.put(outputName,
									new ArrayList<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>>());
					outputMap.get(outputName).add(output);
				} else {
					if (!outputMap.get(outputName).contains(output)) {
						outputMap.get(outputName).add(output);
					}
				}

			}
		}
	}

	/**
	 * Collects and emits a tuple/object to the outputs by reusing a
	 * StreamRecord object.
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
	 * Emits a StreamRecord to the outputs.
	 * 
	 * @param streamRecord
	 *            StreamRecord to emit.
	 */
	private void emit(StreamRecord<OUT> streamRecord) {
		streamRecord.newId(channelID);
		serializationDelegate.setInstance(streamRecord);
		emitToOutputs();
	}

	protected void emitToOutputs() {
		for (RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output : outputs) {
			try {
				output.emit(serializationDelegate);
			} catch (Exception e) {
				if (LOG.isErrorEnabled()) {
					LOG.error("Emit failed due to: {}", StringUtils.stringifyException(e));
				}
			}
		}
	}

	@Override
	public void close() {
	}
}
