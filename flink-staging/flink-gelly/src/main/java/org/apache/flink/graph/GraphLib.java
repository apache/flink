package flink.graphs;


import flink.graphs.Graph;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.spargel.java.MessageIterator;
import org.apache.flink.spargel.java.MessagingFunction;
import org.apache.flink.spargel.java.OutgoingEdge;
import org.apache.flink.spargel.java.VertexUpdateFunction;

import java.io.Serializable;

public class GraphLib {


    /**
     * Function that updates the rank of a vertex by summing up the partial ranks from all incoming messages
     * and then applying the dampening formula.
     */
    public static final class VertexRankUpdater<K extends Comparable<K> & Serializable> extends VertexUpdateFunction<K, Double, Double> {

        private final long numVertices;
        private final double beta;

        public VertexRankUpdater(long numVertices, double beta) {
            this.numVertices = numVertices;
            this.beta = beta;
        }

        @Override
        public void updateVertex(K vertexKey, Double vertexValue, MessageIterator<Double> inMessages) {
            double rankSum = 0.0;
            for (double msg : inMessages) {
                rankSum += msg;
            }

            // apply the dampening factor / random jump
            double newRank = (beta * rankSum) + (1-beta)/numVertices;
            setNewVertexValue(newRank);
        }
    }

    /**
     * Distributes the rank of a vertex among all target vertices according to the transition probability,
     * which is associated with an edge as the edge value.
     */
    public static final class RankMessenger<K extends Comparable<K> & Serializable> extends MessagingFunction<K, Double, Double, Double> {

        @Override
        public void sendMessages(K vertexId, Double newRank) {
            for (OutgoingEdge<K, Double> edge : getOutgoingEdges()) {
                sendMessageTo(edge.target(), newRank * edge.edgeValue());
            }
        }
    }

    //TODO Get numVertices from graph as long when this is possible (https://github.com/apache/incubator-flink/pull/210)
    public static <K extends Comparable<K> & Serializable> Graph<K,Double,Double> pageRank (Graph<K,Double,Double> network,
        long numVertices, double beta, int maxIterations) {

        return network.runVertexCentricIteration(
                new VertexRankUpdater<K>(numVertices, beta),
                new RankMessenger<K>(),
                maxIterations
        );
    }
}
