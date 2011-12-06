package eu.stratosphere.nephele.execution;

import java.util.Queue;

import eu.stratosphere.nephele.types.Record;

public interface Mapper<I extends Record, O extends Record> {

	void map(I input) throws Exception;

	Queue<O> getOutputCollector();
	
	void close();
}
