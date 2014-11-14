package org.apache.flink.api.java.aggregation;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;

public class CountAggregationFunction<T> extends AggregationFunction<T, Long> {
	private static final long serialVersionUID = 514373288927267811L;

	long count;
	
	@Override
	public ResultTypeBehavior getResultTypeBehavior() {
		return ResultTypeBehavior.FIXED;
	}

	@Override
	public BasicTypeInfo<Long> getResultType() {
		return BasicTypeInfo.LONG_TYPE_INFO;
	}

	@Override
	public void setInputType(BasicTypeInfo<T> inputType) { }

	@Override
	public int getFieldPosition() { return 0; }

	@Override
	public void initialize() {
		count = 0L;
	}

	@Override
	public void aggregate(T value) {
		count += 1;
	}

	@Override
	public Long getAggregate() {
		return count;
	}

}
