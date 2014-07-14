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

import java.util.List;

import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.pact.runtime.plugable.SerializationDelegate;
import eu.stratosphere.streaming.api.streamrecord.ArrayStreamRecord;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.util.Collector;

public class StreamCollector<T extends Tuple> implements Collector<T> {

	protected StreamRecord streamRecord;
	protected int batchSize;
	protected long batchTimeout;
	protected int counter = 0;
	protected int channelID;
	private long timeOfLastRecordEmitted = System.currentTimeMillis();;
	private List<RecordWriter<StreamRecord>> outputs;

	public StreamCollector(int batchSize, long batchTimeout, int channelID,
			SerializationDelegate<Tuple> serializationDelegate,
			List<RecordWriter<StreamRecord>> outputs) {
		this.batchSize = batchSize;
		this.batchTimeout = batchTimeout;
		this.streamRecord = new ArrayStreamRecord(batchSize);
		this.streamRecord.setSeralizationDelegate(serializationDelegate);
		this.channelID = channelID;
		this.outputs = outputs;
	}

	public StreamCollector(int batchSize, long batchTimeout, int channelID,
			SerializationDelegate<Tuple> serializationDelegate) {
		this(batchSize, batchTimeout, channelID, serializationDelegate, null);
	}

	// TODO reconsider emitting mechanism at timeout (find a place to timeout)
	@Override
	public void collect(T tuple) {
		//TODO: move copy to StreamCollector2
		streamRecord.setTuple(counter, StreamRecord.copyTuple(tuple));
		counter++;

		if (counter >= batchSize) {
			emit(streamRecord);
			// timeOfLastRecordEmitted = System.currentTimeMillis();
		} else {
			// timeout();
		}
	}

	public void timeout() {
		if (timeOfLastRecordEmitted + batchTimeout < System.currentTimeMillis()) {
			StreamRecord truncatedRecord = new ArrayStreamRecord(streamRecord, counter);
			emit(truncatedRecord);
			timeOfLastRecordEmitted = System.currentTimeMillis();
		}
	}

	private void emit(StreamRecord streamRecord) {
		counter = 0;
		streamRecord.setId(channelID);

		if (outputs == null) {
			System.out.println(streamRecord);
		} else {
			for (RecordWriter<StreamRecord> output : outputs) {
				try {
					output.emit(streamRecord);
					output.flush();
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println("emit fail");
				}
			}
		}
	}

	@Override
	public void close() {

	}

}
