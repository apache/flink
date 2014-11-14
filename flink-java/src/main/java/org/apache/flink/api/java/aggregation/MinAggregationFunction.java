package org.apache.flink.api.java.aggregation;


public class MinAggregationFunction<T extends Comparable<T>> extends InputTypeAggregationFunction<T> {
	private static final long serialVersionUID = -1587835317837938137L;

	private T min;
	
	public MinAggregationFunction(int field) {
		super(field);
	}
	
	@Override
	public void initialize() {
		min = null;
	}

	@Override
	public void aggregate(T value) {
		if (min == null || min.compareTo(value) > 0) {
			min = value;
		}
	}

	@Override
	public T getAggregate() {
		return min;
	}

}
