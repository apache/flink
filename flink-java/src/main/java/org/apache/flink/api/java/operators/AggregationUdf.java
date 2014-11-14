package org.apache.flink.api.java.operators;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.aggregation.AggregationFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.util.Collector;

public class AggregationUdf<T, R extends Tuple> implements GroupReduceFunction<T, R>, Serializable {
	private static final long serialVersionUID = 5563658873455921533L;

	private AggregationFunction<?, ?>[] functions;
	private R result;
	
	public AggregationUdf(R result, AggregationFunction<?, ?>... functions) {
		this.result = result;
		this.functions = functions;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void reduce(Iterable<T> records, Collector<R> out) throws Exception {
		for (AggregationFunction<?, ?> function : functions) {
			function.initialize();
		}
		
		Tuple current = null;
		Iterator<T> values = records.iterator();
		while (values.hasNext()) {
			current = (Tuple) values.next();
			for (AggregationFunction function : functions) {
				int fieldPosition = function.getFieldPosition();
				Object fieldValue = current.getFieldNotNull(fieldPosition);
				function.aggregate(fieldValue);
			}
		}
		
		int index = 0;
		for (AggregationFunction<?, ?> function : functions) {
			Object aggregatedValue = function.getAggregate();
			result.setField(aggregatedValue, index);
			index += 1;
		}
		out.collect(result);
	}
}