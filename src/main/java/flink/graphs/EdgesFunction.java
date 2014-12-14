package flink.graphs;

import java.io.Serializable;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.tuple.Tuple2;

public interface EdgesFunction<K extends Comparable<K> & Serializable, 
	EV extends Serializable, O> extends Function, Serializable {

	Tuple2<K, O> iterateEdges(Iterable<Tuple2<K, Edge<K, EV>>> edges) throws Exception;
}
