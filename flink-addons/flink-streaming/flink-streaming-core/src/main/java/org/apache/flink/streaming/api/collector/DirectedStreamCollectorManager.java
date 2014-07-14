/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package org.apache.flink.streaming.api.collector;

import java.util.HashMap;
import java.util.List;

import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.io.network.api.RecordWriter;

public class DirectedStreamCollectorManager<T extends Tuple> extends StreamCollectorManager<T> {

	OutputSelector<T> outputSelector;
	HashMap<String, AbstractCollector<Tuple>> outputNameMap;

	public DirectedStreamCollectorManager(SerializationDelegate<Tuple> serializationDelegate,
			int channelID, long batchTimeout,OutputSelector<T> outputSelector) {
		super(serializationDelegate, channelID, batchTimeout);
		this.outputSelector = outputSelector;
		this.outputNameMap = new HashMap<String, AbstractCollector<Tuple>>();
	}

	@Override
	public void addNotPartitionedCollector(RecordWriter<StreamRecord> output, int batchSize,
			String outputName) {
		if (outputName != null) {
			outputNameMap.put(outputName, addNotPartitionedCollector(output, batchSize));
		}
	}

	@Override
	public void addPartitionedCollector(RecordWriter<StreamRecord> output, int parallelism,
			int keyPosition, int batchSize, String outputName) {
		if (outputName != null) {
			outputNameMap.put(outputName,
					addPartitionedCollector(output, parallelism, keyPosition, batchSize));
		}
	}

	// TODO make this method faster
	@Override
	public void collect(T tuple) {
		T copiedTuple = StreamRecord.copyTuple(tuple);

		List<String> outputs = (List<String>) outputSelector.getOutputs(tuple);

		for (String outputName : outputs) {
			AbstractCollector<Tuple> output = outputNameMap.get(outputName);
			if (output != null) {
				output.collect(copiedTuple);
			} else {
				if (log.isErrorEnabled()) {
					log.error("Undefined output name given by OutputSelector (" + outputName
							+ "), collecting is omitted.");
				}
			}
		}

		outputSelector.clearList();
	}
}
