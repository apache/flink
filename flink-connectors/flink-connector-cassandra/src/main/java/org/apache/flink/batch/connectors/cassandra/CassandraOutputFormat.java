package org.apache.flink.batch.connectors.cassandra;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

/**
 * Same as CassandraTupleOutputFormat, will be deprecated.
 * @param <OUT>
 */
@Deprecated
public class CassandraOutputFormat<OUT extends Tuple> extends CassandraTupleOutputFormat<OUT> {
	public CassandraOutputFormat(String insertQuery, ClusterBuilder builder) {
		super(insertQuery, builder);
	}
}
