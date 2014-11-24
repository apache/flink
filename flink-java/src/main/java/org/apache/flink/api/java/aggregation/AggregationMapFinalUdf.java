package org.apache.flink.api.java.aggregation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;

public class AggregationMapFinalUdf<IN extends Tuple, OUT extends Tuple> extends AggregationUdfBase<OUT> implements MapFunction<IN, OUT>{
	private static final long serialVersionUID = 6792518274017437410L;

	private AggregationFunction<?, ?>[] functions;

	public AggregationMapFinalUdf(AggregationFunction<?, ?>[] functions) {
    	super(functions.length);
		this.functions = functions;
	}

	@Override
	public OUT map(IN input) throws Exception {
		OUT result = createResultTuple();
		for (AggregationFunction<?, ?> function : functions) {
			int outputPosition = function.getOutputPosition();
			Object value = null;
			if (function instanceof CompositeAggregationFunction) {
				CompositeAggregationFunction<?, ?> composite = (CompositeAggregationFunction<?, ?>) function;
				value = composite.computeComposite(input);
			} else {
				int intermediatePosition = function.getIntermediatePosition();
				value = input.getField(intermediatePosition);
			}
			result.setField(value, outputPosition);
		}
		return result;
	}

}
