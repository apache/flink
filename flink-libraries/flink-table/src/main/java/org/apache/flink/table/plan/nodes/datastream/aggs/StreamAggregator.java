package org.apache.flink.table.plan.nodes.datastream.aggs;

import org.apache.flink.api.java.summarize.aggregation.Aggregator;

public interface StreamAggregator<T, R> extends Aggregator<T, R> {

	/**
	 * Some types of aggregations need to be recomputed along window element
	 * whereas other can just rely on element eviction policy. The reset method
	 * sets the aggregation value to its initial status, before any aggregation 
	 * was executed.
	 */
	void reset();
	
	/**
	 * When any type of sliding window purges elements, the aggregation function
	 * can update the result by invoking this method and implementing the necessary business logic.
	 * E.g. a sum aggregation in a window can subtract from the sum the evicted value. 
	 * @param evictedValue
	 */
	void evictElement(T evictedValue);
	
}
