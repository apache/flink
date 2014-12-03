package flink.graphs;

import java.io.Serializable;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class VertexToTuple2Map<K extends Comparable<K> & Serializable, 
	VV extends Serializable> implements MapFunction<Vertex<K, VV>, Tuple2<K, VV>> {

	private static final long serialVersionUID = 1L;

	public Tuple2<K, VV> map(Vertex<K, VV> vertex) {
		return new Tuple2<K, VV>(vertex.f0, vertex.f1);
	}

}
