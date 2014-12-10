package flink.graphs;


import java.io.Serializable;

public interface GraphAlgorithm<K extends Comparable<K> & Serializable, VV extends Serializable,
        EV extends Serializable> {

    public Graph<K,VV,EV> run (Graph<K,VV,EV> input);

}
