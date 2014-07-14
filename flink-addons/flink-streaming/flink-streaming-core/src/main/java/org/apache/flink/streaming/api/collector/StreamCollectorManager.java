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

package org.apache.flink.streaming.api.collector;

import java.util.Collection;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.streaming.api.streamcomponent.AbstractCollector;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.io.network.api.RecordWriter;
import org.apache.flink.util.Collector;

public class StreamCollectorManager<T extends Tuple> implements Collector<T> {

	protected static final Log log = LogFactory.getLog(StreamCollectorManager.class);
	private Collection<AbstractCollector<Tuple>> collectors;
	private SerializationDelegate<Tuple> serializationDelegate;
	protected int channelID;
	protected int keyPosition;
	private long batchTimeout;

	public StreamCollectorManager(SerializationDelegate<Tuple> serializationDelegate,
			int channelID, long batchTimeout) {
		this.collectors = new HashSet<AbstractCollector<Tuple>>();
		this.serializationDelegate = serializationDelegate;
		this.channelID = channelID;
		this.batchTimeout = batchTimeout;
	}

	protected PartitionedCollector<Tuple> addPartitionedCollector(
			RecordWriter<StreamRecord> output, int outputParallelism, int keyPosition, int batchSize) {
		@SuppressWarnings("unchecked")
		StreamCollector<Tuple>[] collector = new StreamCollector[outputParallelism];

		for (int j = 0; j < outputParallelism; j++) {
			collector[j] = new StreamCollector<Tuple>(batchSize, batchTimeout, channelID,
					serializationDelegate, output, j);
		}

		PartitionedCollector<Tuple> partitionedCollector = new PartitionedCollector<Tuple>(
				collector, keyPosition);
		collectors.add(partitionedCollector);
		return partitionedCollector;
	}

	protected NotPartitionedCollector<Tuple> addNotPartitionedCollector(
			RecordWriter<StreamRecord> output, int batchSize) {

		StreamCollector<Tuple> collector = new StreamCollector<Tuple>(batchSize, batchTimeout,
				channelID, serializationDelegate, output, 0);
		NotPartitionedCollector<Tuple> notPartitionedCollector = new NotPartitionedCollector<Tuple>(
				collector);
		collectors.add(notPartitionedCollector);
		return notPartitionedCollector;
	}

	public void addPartitionedCollector(RecordWriter<StreamRecord> output, int parallelism,
			int keyPosition, int batchSize, String outputName) {
		addPartitionedCollector(output, parallelism, keyPosition, batchSize);
	}

	public void addNotPartitionedCollector(RecordWriter<StreamRecord> output, int batchSize,
			String outputName) {
		addNotPartitionedCollector(output, batchSize);
	}

	@Override
	public void collect(T tuple) {
		T copiedTuple = StreamRecord.copyTuple(tuple);

		for (AbstractCollector<Tuple> collector : collectors) {
			collector.collect(copiedTuple);
		}
	}

	@Override
	public void close() {
	}
}
