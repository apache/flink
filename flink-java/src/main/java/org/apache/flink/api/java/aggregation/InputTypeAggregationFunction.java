package org.apache.flink.api.java.aggregation;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;

public abstract class InputTypeAggregationFunction<T>
		extends FieldAggregationFunction<T, T> {
	private static final long serialVersionUID = -3129049288957424646L;

	private Class<T> type = null;
	
	public InputTypeAggregationFunction(int field) {
		super(field);
	}

	@Override
	public ResultTypeBehavior getResultTypeBehavior() {
		return ResultTypeBehavior.INPUT;
	}

	@Override
	public BasicTypeInfo<T> getResultType() {
		return BasicTypeInfo.getInfoFor(type);
	}

	@Override
	public void setInputType(BasicTypeInfo<T> inputType) {
		type = inputType.getTypeClass();
	}

}