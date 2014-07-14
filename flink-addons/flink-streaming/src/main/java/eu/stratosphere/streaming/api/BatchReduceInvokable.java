package eu.stratosphere.streaming.api;

import java.util.Iterator;

import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

public class BatchReduceInvokable<IN extends Tuple, OUT extends Tuple> extends UserTaskInvokable<IN, OUT> {
	private static final long serialVersionUID = 1L;
	
	private GroupReduceFunction<IN, OUT> reducer;
	public BatchReduceInvokable(GroupReduceFunction<IN, OUT> reduceFunction) {
		this.reducer = reduceFunction; 
	}
	
	@Override
	public void invoke(StreamRecord record, StreamCollector<OUT> collector) throws Exception {
		Iterator<IN> iterator = (Iterator<IN>) record.getBatchIterable().iterator();
		reducer.reduce(iterator, collector);
	}
	
}
