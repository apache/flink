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

import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.pact.runtime.plugable.SerializationDelegate;
import eu.stratosphere.streaming.api.streamrecord.ArrayStreamRecord;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.util.Collector;

public class StreamCollector implements Collector<Tuple> {

	protected StreamRecord streamRecord;
	protected int batchSize;
	protected int counter = 0;
	protected int channelID;

	public StreamCollector(int batchSize, int channelID,
			SerializationDelegate<Tuple> serializationDelegate) {
		this.batchSize = batchSize;
		this.streamRecord = new ArrayStreamRecord(batchSize);
		this.streamRecord.setSeralizationDelegate(serializationDelegate);
		this.channelID = channelID;
	}

	@Override
	public void collect(Tuple tuple) {
		streamRecord.setTuple(counter, StreamRecord.copyTuple(tuple));
		counter++;
		if (counter >= batchSize) {
			counter = 0;
			streamRecord.setId(channelID);
			emit(streamRecord);
		}
	}

	private void emit(StreamRecord streamRecord) {
		System.out.println(streamRecord);
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}
