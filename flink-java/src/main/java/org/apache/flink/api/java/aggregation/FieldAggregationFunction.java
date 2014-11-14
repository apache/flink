package org.apache.flink.api.java.aggregation;

public abstract class FieldAggregationFunction<T, R> extends
		AggregationFunction<T, R> {
	private static final long serialVersionUID = -7118813879097201489L;

	private int fieldPosition;
	
	protected FieldAggregationFunction() {
		super();
	}
	
	public FieldAggregationFunction(int fieldPosition) {
		this.fieldPosition = fieldPosition;
	}
	
	@Override
	public int getFieldPosition() {
		return fieldPosition;
	}

	protected void setFieldPosition(int fieldPosition) {
		this.fieldPosition = fieldPosition;
	}

}
