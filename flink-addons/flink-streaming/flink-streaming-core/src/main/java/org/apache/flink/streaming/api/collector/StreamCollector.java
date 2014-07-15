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

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.runtime.io.network.api.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

public class StreamCollector<T extends Tuple> implements Collector<T> {

	protected StreamRecord<T> streamRecord;
	protected int channelID;
	private List<RecordWriter<StreamRecord<T>>> outputs;
	protected Map<String, RecordWriter<StreamRecord<T>>> outputMap;

	public StreamCollector(int channelID, SerializationDelegate<T> serializationDelegate) {

		this.streamRecord = new StreamRecord<T>();
		this.streamRecord.setSeralizationDelegate(serializationDelegate);
		this.channelID = channelID;
		this.outputs = new ArrayList<RecordWriter<StreamRecord<T>>>();
		this.outputMap = new HashMap<String, RecordWriter<StreamRecord<T>>>();
	}

	public void addOutput(RecordWriter<StreamRecord<T>> output, String outputName) {
		outputs.add(output);
		if (outputName != null) {
			outputMap.put(outputName, output);
		}
	}

	@Override
	public void collect(T tuple) {
		streamRecord.setTuple(tuple);
		emit(streamRecord);
	}

	private void emit(StreamRecord<T> streamRecord) {
		streamRecord.setId(channelID);
		for (RecordWriter<StreamRecord<T>> output : outputs) {
			try {
				output.emit(streamRecord);
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("emit fail");
			}
		}
	}

	@Override
	public void close() {
	}
}
