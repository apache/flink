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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.runtime.io.network.api.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

/**
 * Collector for tuples in Apache Flink stream processing. The collected tuples
 * will be wrapped with ID in a {@link StreamRecord} and then emitted to the
 * outputs.
 * 
 * @param <T>
 *            Type of the Tuple collected.
 */
public class StreamCollector<T extends Tuple> implements Collector<T> {

	private static final Log LOG = LogFactory.getLog(StreamCollector.class);

	protected StreamRecord<T> streamRecord;
	protected int channelID;
	private List<RecordWriter<SerializationDelegate<StreamRecord<T>>>> outputs;
	protected Map<String, List<RecordWriter<SerializationDelegate<StreamRecord<T>>>>> outputMap;
	protected SerializationDelegate<StreamRecord<T>> serializationDelegate;

	/**
	 * Creates a new StreamCollector
	 * 
	 * @param channelID
	 *            Channel ID of the Task
	 * @param serializationDelegate
	 *            Serialization delegate used for tuple serialization
	 */
	public StreamCollector(int channelID, SerializationDelegate<StreamRecord<T>> serializationDelegate) {

		this.serializationDelegate = serializationDelegate;
		this.streamRecord = new StreamRecord<T>();
		this.channelID = channelID;
		this.outputs = new ArrayList<RecordWriter<SerializationDelegate<StreamRecord<T>>>>();
		this.outputMap = new HashMap<String, List<RecordWriter<SerializationDelegate<StreamRecord<T>>>>>();
	}

	/**
	 * Adds an output with the given user defined name
	 * 
	 * @param output
	 *            The RecordWriter object representing the output.
	 * @param outputName
	 *            User defined name of the output.
	 */
	public void addOutput(RecordWriter<SerializationDelegate<StreamRecord<T>>> output,
			String outputName) {
		outputs.add(output);
		if (outputName != null) {
			if (outputMap.containsKey(outputName)) {
				outputMap.get(outputName).add(output);
			} else {
				outputMap.put(outputName,
						new ArrayList<RecordWriter<SerializationDelegate<StreamRecord<T>>>>());
				outputMap.get(outputName).add(output);
			}

		}
	}

	/**
	 * Collects and emits a tuple to the outputs by reusing a StreamRecord
	 * object.
	 * 
	 * @param tuple
	 *            Tuple to be collected and emitted.
	 */
	@Override
	public void collect(T tuple) {
		streamRecord.setTuple(tuple);
		emit(streamRecord);
	}

	/**
	 * Emits a StreamRecord to all the outputs.
	 * 
	 * @param streamRecord
	 *            StreamRecord to emit.
	 */
	private void emit(StreamRecord<T> streamRecord) {
		streamRecord.setId(channelID);
		serializationDelegate.setInstance(streamRecord);
		for (RecordWriter<SerializationDelegate<StreamRecord<T>>> output : outputs) {
			try {
				output.emit(serializationDelegate);
			} catch (Exception e) {
				if (LOG.isErrorEnabled()) {
					LOG.error(String.format("Emit failed due to: %s",
							StringUtils.stringifyException(e)));
				}
			}
		}
	}

	@Override
	public void close() {
	}
}
