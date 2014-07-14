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
package eu.stratosphere.streaming.api.streamrecord;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.pact.runtime.plugable.SerializationDelegate;
import eu.stratosphere.runtime.io.api.RecordWriter;
import eu.stratosphere.streaming.api.OutputSelector;
import eu.stratosphere.util.Collector;

public class DirectedStreamCollectorManager<T extends Tuple> extends StreamCollectorManager<T> {

	OutputSelector<T> outputSelector;
	HashMap<String, StreamCollector<Tuple>> notPartitionedOutputNameMap;
	HashMap<String, StreamCollector<Tuple>[]> partitionedOutputNameMap;

	public DirectedStreamCollectorManager(List<Integer> batchSizesOfNotPartitioned,
			List<Integer> batchSizesOfPartitioned, List<Integer> parallelismOfOutput,
			int keyPosition, long batchTimeout, int channelID,
			SerializationDelegate<Tuple> serializationDelegate,
			List<RecordWriter<StreamRecord>> partitionedOutputs,
			List<RecordWriter<StreamRecord>> notPartitionedOutputs,
			OutputSelector<T> outputSelector, List<String> partitionedOutputNames,
			List<String> notPartitionedOutputNames) {
		super(batchSizesOfNotPartitioned, batchSizesOfPartitioned, parallelismOfOutput,
				keyPosition, batchTimeout, channelID, serializationDelegate, partitionedOutputs,
				notPartitionedOutputs);
		
		this.outputSelector = outputSelector;
		this.notPartitionedOutputNameMap = new HashMap<String, StreamCollector<Tuple>>();
		this.partitionedOutputNameMap = new HashMap<String, StreamCollector<Tuple>[]>();
		
		// TODO init outputNameMap
		partitionedCollectors = new HashSet<StreamCollector<Tuple>[]>(
				batchSizesOfPartitioned.size());
		for (int i = 0; i < batchSizesOfPartitioned.size(); i++) {
			@SuppressWarnings("unchecked")
			StreamCollector<Tuple>[] collectors = new StreamCollector[parallelismOfOutput.get(i)];
			for (int j = 0; j < collectors.length; j++) {
				collectors[j] = new StreamCollector<Tuple>(batchSizesOfPartitioned.get(i),
						batchTimeout, channelID, serializationDelegate, partitionedOutputs.get(i));
			}
			partitionedOutputNameMap.put(partitionedOutputNames.get(i), collectors);
			partitionedCollectors.add(collectors);
		}
		
		notPartitionedCollectors = new HashSet<StreamCollector<Tuple>>(
				batchSizesOfNotPartitioned.size());
		for (int i = 0; i < batchSizesOfNotPartitioned.size(); i++) {
			StreamCollector<Tuple> collector = new StreamCollector<Tuple>(batchSizesOfNotPartitioned
					.get(i), batchTimeout, channelID, serializationDelegate, notPartitionedOutputs
					.get(i));
			notPartitionedCollectors.add(collector);
			notPartitionedOutputNameMap
			.put(
					notPartitionedOutputNames
					.get(i),
					collector);
		}
	}

	@Override
	protected void setPartitionedCollectors(List<Integer> batchSizesOfNotPartitioned,
			List<Integer> batchSizesOfPartitioned, List<Integer> parallelismOfOutput,
			int keyPosition, long batchTimeout, int channelID,
			SerializationDelegate<Tuple> serializationDelegate,
			List<RecordWriter<StreamRecord>> partitionedOutputs,
			List<RecordWriter<StreamRecord>> notPartitionedOutputs) {

	}
	
	@Override
	protected void setNotPartitionedCollectors(List<Integer> batchSizesOfNotPartitioned,
			List<Integer> batchSizesOfPartitioned, List<Integer> parallelismOfOutput,
			int keyPosition, long batchTimeout, int channelID,
			SerializationDelegate<Tuple> serializationDelegate,
			List<RecordWriter<StreamRecord>> partitionedOutputs,
			List<RecordWriter<StreamRecord>> notPartitionedOutputs) {

	}

	// TODO make this method faster
	@Override
	public void collect(T tuple) {
		T copiedTuple = StreamRecord.copyTuple(tuple);

		List<String> outputs = outputSelector.select(tuple);

		int partitionHash = Math.abs(copiedTuple.getField(keyPostition).hashCode());

		for (String outputName : outputs) {
			Collector<Tuple> notPartitionedCollector = notPartitionedOutputNameMap.get(outputName);
			Collector<Tuple>[] partitionedCollector = partitionedOutputNameMap.get(outputName);

			if (notPartitionedCollector != null) {
				notPartitionedCollector.collect(copiedTuple);
			} else if (partitionedCollector != null) {
				partitionedCollector[partitionHash % partitionedCollector.length]
						.collect(copiedTuple);
			}
		}
	}
}
