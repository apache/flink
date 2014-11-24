package org.apache.flink.api.java.aggregation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;

public class AggregationMapIntermediateUdf<IN extends Tuple, OUT extends Tuple> extends AggregationUdfBase<OUT> implements MapFunction<IN, OUT>{
	private static final long serialVersionUID = 3326127349510588787L;

	private AggregationFunction<?, ?>[] functions;
	
    public AggregationMapIntermediateUdf(AggregationFunction<?, ?>[] functions) {
    	super(functions.length);
		this.functions = functions;
	}
	
    @Override
	public OUT map(IN value) throws Exception {
		OUT result = createResultTuple();
		for (AggregationFunction<?, ?> function : functions) {
			processAggregationFunction(function, value, result);
		}
		return result;
	}
	
	// helper function to capture the generic types of AggregationFunction
	private <T, R> void processAggregationFunction(
			AggregationFunction<T, R> function, IN value, OUT result) {
		int inputPosition = function.getInputPosition();
		int intermediatePosition = function.getIntermediatePosition();
		T inputValue = value.getField(inputPosition);
		R outputValue = function.initialize(inputValue);
		result.setField(outputValue, intermediatePosition);
	}

}
