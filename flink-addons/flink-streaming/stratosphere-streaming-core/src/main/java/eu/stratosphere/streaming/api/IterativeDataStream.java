package eu.stratosphere.streaming.api;

import eu.stratosphere.api.java.tuple.Tuple;

public class IterativeDataStream<T extends Tuple> {
	
	private final StreamExecutionEnvironment environment;

	public IterativeDataStream(StreamExecutionEnvironment environment){
		this.environment = environment;
	}
	
	public DataStream<T> closeWith(DataStream<T> iterationResult) {
		return environment.closeIteration(iterationResult.copy());
	}
	
}