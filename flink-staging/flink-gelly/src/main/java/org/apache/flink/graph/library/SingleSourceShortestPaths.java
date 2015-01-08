package flink.graphs.library;

import flink.graphs.*;
import flink.graphs.spargel.MessageIterator;
import flink.graphs.spargel.MessagingFunction;
import flink.graphs.spargel.OutgoingEdge;
import flink.graphs.spargel.VertexUpdateFunction;

import org.apache.flink.api.common.functions.MapFunction;

import java.io.Serializable;

@SuppressWarnings("serial")
public class SingleSourceShortestPaths<K extends Comparable<K> & Serializable> implements GraphAlgorithm<K, Double, Double> {

    private final K srcVertexId;
    private final Integer maxIterations;

    public SingleSourceShortestPaths(K srcVertexId, Integer maxIterations) {
        this.srcVertexId = srcVertexId;
        this.maxIterations = maxIterations;
    }

    @Override
    public Graph<K, Double, Double> run(Graph<K, Double, Double> input) {

    	return input.mapVertices(new InitVerticesMapper<K>(srcVertexId))
    			.runVertexCentricIteration(
                new VertexDistanceUpdater<K>(),
                new MinDistanceMessenger<K>(),
                maxIterations
        );
    }

    public static final class InitVerticesMapper<K extends Comparable<K> & Serializable> 
    	implements MapFunction<Vertex<K,Double>, Double> {

    	private K srcVertexId;

    	public InitVerticesMapper(K srcId) {
    		this.srcVertexId = srcId;
    	}
    		
		public Double map(Vertex<K, Double> value) {
			if (value.f0.equals(srcVertexId)) {
				return 0.0;
			}
			else {
				return Double.MAX_VALUE;
			}
		}
    }

    /**
     * Function that updates the value of a vertex by picking the minimum distance from all incoming messages.
     *
     * @param <K>
     */
    public static final class VertexDistanceUpdater<K extends Comparable<K> & Serializable>
            extends VertexUpdateFunction<K, Double, Double> {

        @Override
        public void updateVertex(K vertexKey, Double vertexValue, MessageIterator<Double> inMessages) {

            Double minDistance = Double.MAX_VALUE;

            for (double msg : inMessages) {
                if (msg < minDistance) {
                    minDistance = msg;
                }
            }

            if (vertexValue > minDistance) {
                setNewVertexValue(minDistance);
            }
        }
    }

    /**
     * Distributes the minimum distance associated with a given vertex among all the target vertices
     * summed up with the edge's value.
     *
     * @param <K>
     */
    public static final class MinDistanceMessenger<K extends Comparable<K> & Serializable>
            extends MessagingFunction<K, Double, Double, Double> {

        @Override
        public void sendMessages(K vertexKey, Double newDistance) throws Exception {
            for (OutgoingEdge<K, Double> edge : getOutgoingEdges()) {
                sendMessageTo(edge.target(), newDistance + edge.edgeValue());
            }
        }
    }
}