package org.apache.flink.addons.hbase;

import org.apache.flink.api.java.tuple.Tuple;

import org.apache.hadoop.hbase.client.Result;

/**
 * HBase scanned {@link Result} to {@link Tuple} mapper.
 */
public interface ResultToTupleMapper<T extends Tuple> {

	/**
	 * The output from HBase is always an instance of {@link Result}.
	 * This method is to copy the data in the Result instance into the required {@link Tuple}
	 * @param r The Result instance from HBase that needs to be converted
	 * @return The appropriate instance of {@link Tuple} that contains the needed information.
	 */
	T mapResultToTuple(Result r);
}
