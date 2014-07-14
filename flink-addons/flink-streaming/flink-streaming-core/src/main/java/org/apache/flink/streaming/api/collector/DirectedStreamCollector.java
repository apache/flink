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

import java.util.Collection;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.runtime.io.network.api.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;

public class DirectedStreamCollector extends StreamCollector {

	OutputSelector outputSelector;

	public DirectedStreamCollector(int channelID,
			SerializationDelegate<Tuple> serializationDelegate, OutputSelector outputSelector) {
		super(channelID, serializationDelegate);
		this.outputSelector = outputSelector;

	}

	@Override
	public void collect(Tuple tuple) {
		streamRecord.setTuple(tuple);
		emit(streamRecord);
	}

	private void emit(StreamRecord streamRecord) {
		Collection<String> outputNames = outputSelector.getOutputs(streamRecord.getTuple());
		streamRecord.setId(channelID);
		for (String outputName : outputNames) {
			try {
				outputMap.get(outputName).emit(streamRecord);
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("emit fail");
			}
		}
		outputNames.clear();

	}
}
