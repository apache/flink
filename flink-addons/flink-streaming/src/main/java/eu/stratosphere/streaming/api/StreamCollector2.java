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

package eu.stratosphere.streaming.api;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.pact.runtime.plugable.SerializationDelegate;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.util.Collector;

public class StreamCollector2<T extends Tuple> implements Collector<T> {

	ArrayList<StreamCollector<Tuple>> notPartitionedCollectors;
	ArrayList<StreamCollector<Tuple>[]> partitionedCollectors;
	int keyPostition;
	
	// TODO consider channelID
	public StreamCollector2(int[] batchSizesOfNotPartitioned, int[] batchSizesOfPartitioned,
			int[] parallelismOfOutput, int keyPosition, long batchTimeout, int channelID,
			SerializationDelegate<Tuple> serializationDelegate,
			List<RecordWriter<StreamRecord>> outputs) {

		notPartitionedCollectors = new ArrayList<StreamCollector<Tuple>>(
				batchSizesOfNotPartitioned.length);
		partitionedCollectors = new ArrayList<StreamCollector<Tuple>[]>(
				batchSizesOfPartitioned.length);

		this.keyPostition = keyPosition;
		
		for (int i = 0; i < batchSizesOfNotPartitioned.length; i++) {
			notPartitionedCollectors.add(new StreamCollector<Tuple>(batchSizesOfNotPartitioned[i],
					batchTimeout, channelID, serializationDelegate, outputs));
		}

		for (int i = 0; i < batchSizesOfPartitioned.length; i++) {
			StreamCollector<Tuple>[] collectors = new StreamCollector[parallelismOfOutput[i]];
			for (int j = 0; j < collectors.length; j++) {
				collectors[j] = new StreamCollector<Tuple>(batchSizesOfPartitioned[i],
						batchTimeout, channelID, serializationDelegate, outputs);
			}
			partitionedCollectors.add(collectors);
		}
	}
	
	// TODO copy here instead of copying inside every StreamCollector
	@Override
	public void collect(T record) {
		for (StreamCollector<Tuple> collector : notPartitionedCollectors) {
			collector.collect(record);
		}
		
		int partitionHash = Math.abs(record.getField(keyPostition).hashCode());
		
		for (StreamCollector<Tuple>[] collectors : partitionedCollectors) {
			collectors[partitionHash % collectors.length].collect(record);
		}
	}

	@Override
	public void close() {

	}
}
