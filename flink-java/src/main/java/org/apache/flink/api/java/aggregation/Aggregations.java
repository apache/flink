package org.apache.flink.api.java.aggregation;

@SuppressWarnings("rawtypes")
public class Aggregations {

	public static AggregationFunction min(int field) {
		return new MinAggregationFunction(field);
	}
	
	public static AggregationFunction max(int field) {
		return new MaxAggregationFunction(field);
	}
	
	public static AggregationFunction count() {
		return new CountAggregationFunction();
	}
	
	public static AggregationFunction sum(int field) {
		return new SumAggregationFunction(field);
	}
	
	public static AggregationFunction average(int field) {
		return new AverageAggregationFunction(field);
	}
	
	public static AggregationFunction key(int field) {
		return new KeySelectionAggregationFunction(field);
	}
	
}
