package eu.stratosphere.api.datastream;

import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.streaming.api.StreamCollector;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

public class FlatMapInvokable<T extends Tuple, R extends Tuple> extends UserTaskInvokable<T, R> {
	private static final long serialVersionUID = 1L;

	private FlatMapFunction<T, R> flatMapper;
	public FlatMapInvokable(FlatMapFunction<T, R> flatMapper2) {
		this.flatMapper = flatMapper2;
	}
	
	@Override
	public void invoke(StreamRecord record, StreamCollector<R> collector) throws Exception {
		int batchSize = record.getBatchSize();
		for (int i = 0; i < batchSize; i++) {
			T tuple = (T) record.getTuple(i);
			flatMapper.flatMap(tuple, collector);
		}
	}		
}
