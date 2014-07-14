/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.streaming.api.collector;

import java.util.HashMap;
import java.util.List;

import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.pact.runtime.plugable.SerializationDelegate;
import eu.stratosphere.runtime.io.api.RecordWriter;
import eu.stratosphere.streaming.api.streamcomponent.AbstractCollector;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

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
