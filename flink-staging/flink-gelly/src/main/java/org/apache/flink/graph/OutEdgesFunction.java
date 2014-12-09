package flink.graphs;

import java.io.Serializable;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.tuple.Tuple2;

public interface OutEdgesFunction<K extends Comparable<K> & Serializable, 
	VV extends Serializable, EV extends Serializable, O> extends Function, Serializable {

	Tuple2<K, O> iterateOutEdges(Vertex<K, VV> v, Iterable<Edge<K, EV>> outEdges) throws Exception;
}
