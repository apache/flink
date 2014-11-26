package flink.graphs;

import java.io.Serializable;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public interface OutEdgesFunction<K extends Comparable<K> & Serializable, 
	VV extends Serializable, EV extends Serializable, O> extends Function, Serializable {

	O iterateOutEdges(Tuple2<K, VV> v, Iterable<Tuple3<K, K, EV>> outEdges) throws Exception;
}
