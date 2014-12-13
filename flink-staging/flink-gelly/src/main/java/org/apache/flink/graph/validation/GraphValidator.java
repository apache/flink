package flink.graphs.validation;
import java.io.Serializable;

import org.apache.flink.api.java.DataSet;

import flink.graphs.Graph;

/**
 * A validation method for different types of Graphs
 *
 * @param <K>
 * @param <VV>
 * @param <EV>
 */
@SuppressWarnings("serial")
public abstract class GraphValidator<K extends Comparable<K> & Serializable, VV extends Serializable,
        EV extends Serializable> implements Serializable{

    public abstract DataSet<Boolean> validate(Graph<K, VV, EV> graph);

}