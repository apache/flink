package flink.graphs;

import java.io.Serializable;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.tuple.Tuple3;

public interface NeighborsFunction<K extends Comparable<K> & Serializable, VV extends Serializable, 
	EV extends Serializable, O> extends Function, Serializable {

	O iterateNeighbors(Iterable<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> neighbors) throws Exception;
}
