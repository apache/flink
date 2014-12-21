package flink.graphs;

import java.io.Serializable;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.tuple.Tuple2;

public interface NeighborsFunctionWithVertexValue<K extends Comparable<K> & Serializable, VV extends Serializable, 
	EV extends Serializable, O> extends Function, Serializable {

	Tuple2<K, O> iterateNeighbors(Vertex<K, VV> vertex, Iterable<Tuple2<Edge<K, EV>, Vertex<K, VV>>> neighbors) throws Exception;
}
