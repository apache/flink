package flink.graphs;

import java.io.Serializable;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class Tuple2ToVertexMap<K extends Comparable<K> & Serializable, 
	VV extends Serializable> implements MapFunction<Tuple2<K, VV>, Vertex<K, VV>> {

	private static final long serialVersionUID = 1L;

	public Vertex<K, VV> map(Tuple2<K, VV> vertex) {
		return new Vertex<K, VV>(vertex.f0, vertex.f1);
	}

}
