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
