package org.apache.flink.api.java.aggregation;

public class KeySelectionAggregationFunction<T> extends InputTypeAggregationFunction<T> {
	private static final long serialVersionUID = -8410506438464358497L;
	
	T key;
	
	public KeySelectionAggregationFunction(int field) {
		super(field);
	}

	@Override
	public void initialize() {
		key = null;
	}

	@Override
	public void aggregate(T value) {
		if (key == null) {
			key = value;
		}
	}

	@Override
	public T getAggregate() {
		return key;
	}

}
