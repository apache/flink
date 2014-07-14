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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.pact.runtime.plugable.SerializationDelegate;
import eu.stratosphere.runtime.io.api.RecordWriter;
import eu.stratosphere.util.Collector;

public class StreamCollectorManager<T extends Tuple> implements Collector<T> {

	Collection<StreamCollector<Tuple>> notPartitionedCollectors;
	Collection<StreamCollector<Tuple>[]> partitionedCollectors;
	int keyPostition;

	// TODO consider channelID
	public StreamCollectorManager(List<Integer> batchSizesOfNotPartitioned,
			List<Integer> batchSizesOfPartitioned, List<Integer> parallelismOfOutput,
			int keyPosition, long batchTimeout, int channelID,
			SerializationDelegate<Tuple> serializationDelegate,
			List<RecordWriter<StreamRecord>> partitionedOutputs,
			List<RecordWriter<StreamRecord>> notPartitionedOutputs) {

		this.keyPostition = keyPosition;
		setPartitionedCollectors(batchSizesOfNotPartitioned, batchSizesOfPartitioned,
				parallelismOfOutput, keyPosition, batchTimeout, channelID, serializationDelegate,
				partitionedOutputs, notPartitionedOutputs);
		setNotPartitionedCollectors(batchSizesOfNotPartitioned, batchSizesOfPartitioned,
				parallelismOfOutput, keyPosition, batchTimeout, channelID, serializationDelegate,
				partitionedOutputs, notPartitionedOutputs);
	}

	protected void setPartitionedCollectors(List<Integer> batchSizesOfNotPartitioned,
			List<Integer> batchSizesOfPartitioned, List<Integer> parallelismOfOutput,
			int keyPosition, long batchTimeout, int channelID,
			SerializationDelegate<Tuple> serializationDelegate,
			List<RecordWriter<StreamRecord>> partitionedOutputs,
			List<RecordWriter<StreamRecord>> notPartitionedOutputs) {
		partitionedCollectors = new HashSet<StreamCollector<Tuple>[]>(
				batchSizesOfPartitioned.size());
		for (int i = 0; i < batchSizesOfPartitioned.size(); i++) {
			@SuppressWarnings("unchecked")
			StreamCollector<Tuple>[] collectors = new StreamCollector[parallelismOfOutput.get(i)];
			for (int j = 0; j < collectors.length; j++) {
				collectors[j] = new StreamCollector<Tuple>(batchSizesOfPartitioned.get(i),
						batchTimeout, channelID, serializationDelegate, partitionedOutputs.get(i));
			}
			partitionedCollectors.add(collectors);
		}
	}

	protected void setNotPartitionedCollectors(List<Integer> batchSizesOfNotPartitioned,
			List<Integer> batchSizesOfPartitioned, List<Integer> parallelismOfOutput,
			int keyPosition, long batchTimeout, int channelID,
			SerializationDelegate<Tuple> serializationDelegate,
			List<RecordWriter<StreamRecord>> partitionedOutputs,
			List<RecordWriter<StreamRecord>> notPartitionedOutputs) {
		notPartitionedCollectors = new HashSet<StreamCollector<Tuple>>(
				batchSizesOfNotPartitioned.size());
		for (int i = 0; i < batchSizesOfNotPartitioned.size(); i++) {
			notPartitionedCollectors.add(new StreamCollector<Tuple>(batchSizesOfNotPartitioned
					.get(i), batchTimeout, channelID, serializationDelegate, notPartitionedOutputs
					.get(i)));
		}
	}

	@Override
	public void collect(T tuple) {
		T copiedTuple = StreamRecord.copyTuple(tuple);

		for (StreamCollector<Tuple> collector : notPartitionedCollectors) {
			collector.collect(copiedTuple);
		}

		int partitionHash = Math.abs(copiedTuple.getField(keyPostition).hashCode());

		for (StreamCollector<Tuple>[] collectors : partitionedCollectors) {
			collectors[partitionHash % collectors.length].collect(copiedTuple);
		}
	}

	@Override
	public void close() {

	}
}
