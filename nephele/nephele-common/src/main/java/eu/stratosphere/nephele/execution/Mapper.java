package eu.stratosphere.nephele.execution;

import java.util.Queue;

import eu.stratosphere.nephele.types.Record;

public interface Mapper<I extends Record, O extends Record> {

	void map(I input, Queue<O> output) throws Exception;
	
	void close(Queue<O> output);
}
