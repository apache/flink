package org.apache.flink.api.java.aggregation;

import java.util.List;

import org.apache.flink.api.java.tuple.Tuple;

public abstract class CompositeAggregationFunction<T, R> extends FieldAggregationFunction<T, R> {
	private static final long serialVersionUID = 517617558180264806L;

	public CompositeAggregationFunction(String name, int fieldPosition) {
		super(name, fieldPosition);
	}
	
	public abstract List<AggregationFunction<?, ?>> getIntermediateAggregationFunctions();
	
	public abstract R computeComposite(Tuple tuple);
	
}
