package flink.graphs.utils;

import java.io.Serializable;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import flink.graphs.Edge;

/**
 * create an Edge DataSetfrom a Tuple3 dataset
 *
 * @param <K>
 * @param <EV>
 */
public class Tuple3ToEdgeMap<K extends Comparable<K> & Serializable, 
	EV extends Serializable> implements MapFunction<Tuple3<K, K, EV>, Edge<K, EV>> {

	private static final long serialVersionUID = 1L;

	public Edge<K, EV> map(Tuple3<K, K, EV> tuple) {
		return new Edge<K, EV>(tuple.f0, tuple.f1, tuple.f2);
	}

}
