package flink.graphs.library;

import flink.graphs.Edge;
import flink.graphs.Graph;
import flink.graphs.GraphAlgorithm;
import flink.graphs.spargel.MessageIterator;
import flink.graphs.spargel.MessagingFunction;
import flink.graphs.spargel.VertexUpdateFunction;

import java.io.Serializable;

public class PageRank<K extends Comparable<K> & Serializable> implements GraphAlgorithm<K, Double, Double> {

    private long numVertices;
    private double beta;
    private int maxIterations;

    public PageRank(long numVertices, double beta, int maxIterations) {
        this.numVertices = numVertices;
        this.beta = beta;
        this.maxIterations = maxIterations;
    }

    @Override
    public Graph<K, Double, Double> run(Graph<K, Double, Double> network) {
        return network.runVertexCentricIteration(
                new VertexRankUpdater<K>(numVertices, beta),
                new RankMessenger<K>(),
                maxIterations
        );
    }


    /**
     * Function that updates the rank of a vertex by summing up the partial ranks from all incoming messages
     * and then applying the dampening formula.
     */
    @SuppressWarnings("serial")
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
    @SuppressWarnings("serial")
	public static final class RankMessenger<K extends Comparable<K> & Serializable> extends MessagingFunction<K, Double, Double, Double> {

        @Override
        public void sendMessages(K vertexId, Double newRank) {
            for (Edge<K, Double> edge : getOutgoingEdges()) {
                sendMessageTo(edge.getTarget(), newRank * edge.getValue());
            }
        }
    }
}
