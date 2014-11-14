package org.apache.flink.api.java.aggregation;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;

public class AverageAggregationFunction<T extends Number> extends FieldAggregationFunction<T, Double> {
	private static final long serialVersionUID = -3901931046368002202L;

	private SumAggregationFunction<T> sumDelegate;
	private CountAggregationFunction<T> countDelegate;
	
	public AverageAggregationFunction(int field) {
		super(field);
		sumDelegate = new SumAggregationFunction<T>();
		countDelegate = new CountAggregationFunction<T>();
	}
	
	@Override
	public ResultTypeBehavior getResultTypeBehavior() {
		return ResultTypeBehavior.FIXED;
	}

	@Override
	public BasicTypeInfo<Double> getResultType() {
		return BasicTypeInfo.DOUBLE_TYPE_INFO;
	}

	@Override
	public void setInputType(BasicTypeInfo<T> inputType) {
		sumDelegate.setInputType(inputType);
	}

	@Override
	public void initialize() {
		sumDelegate.initialize();
		countDelegate.initialize();
	}

	@Override
	public void aggregate(T value) {
		sumDelegate.aggregate(value);
		countDelegate.aggregate(value);
	}

	@Override
	public Double getAggregate() {
		double sum = sumDelegate.getAggregate().doubleValue();
		double count = countDelegate.getAggregate().doubleValue();
		return sum / count;
	}

}
