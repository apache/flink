package flink.graphs;


import java.io.Serializable;

/**
 * @param <K> key type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public interface GraphAlgorithm<K extends Comparable<K> & Serializable, VV extends Serializable,
        EV extends Serializable> {

    public Graph<K,VV,EV> run (Graph<K,VV,EV> input);

}
