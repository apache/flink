package org.apache.flink.api.java.aggregation;

import java.io.Serializable;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;

public abstract class AggregationFunction<T, R> implements Serializable {
	private static final long serialVersionUID = 9082279166205627942L;

	public static enum ResultTypeBehavior {
		INPUT,
		FIXED
	}

	public abstract ResultTypeBehavior getResultTypeBehavior();
	
	public abstract BasicTypeInfo<R> getResultType();
	
	public abstract void setInputType(BasicTypeInfo<T> inputType);
	
	public abstract int getFieldPosition();
	
	public abstract void initialize();
	
	public abstract void aggregate(T value);
	
	public abstract R getAggregate();

}
