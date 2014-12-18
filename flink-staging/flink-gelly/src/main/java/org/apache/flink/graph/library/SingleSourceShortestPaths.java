package flink.graphs.library;

import flink.graphs.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.spargel.java.MessageIterator;
import org.apache.flink.spargel.java.MessagingFunction;
import org.apache.flink.spargel.java.OutgoingEdge;
import org.apache.flink.spargel.java.VertexUpdateFunction;

import java.io.Serializable;

public class SingleSourceShortestPaths<K extends Comparable<K> & Serializable> implements GraphAlgorithm<K, Double, Double> {

    private final K srcVertexId;
    private final Integer maxIterations;

    public SingleSourceShortestPaths(K srcVertexId, Integer maxIterations) {
        this.srcVertexId = srcVertexId;
        this.maxIterations = maxIterations;
    }

    @Override
    public Graph<K, Double, Double> run(Graph<K, Double, Double> input) {
        DataSet<Vertex<K, Double>> sourceVertex = input.getVertices().filter(
                new SelectVertex<K>(srcVertexId));

        DataSet<Vertex<K, Double>> verticesWithInitialDistance = sourceVertex.cross(input.getVertices())
                .map(new InitSrcVertex<K>());

        Graph<K, Double, Double> graph = Graph.create(verticesWithInitialDistance, input.getEdges(),
                ExecutionEnvironment.getExecutionEnvironment());

        return graph.runVertexCentricIteration(
                new VertexDistanceUpdater<K>(),
                new MinDistanceMessenger<K>(),
                maxIterations
        );
    }


    /**
     * Function that updates the value of a vertex by picking the minimum distance from all incoming messages.
     *
     * @param <K>
     */
    @SuppressWarnings("serial")
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
                vertexValue = minDistance;
            }

            setNewVertexValue(vertexValue);
        }
    }

    /**
     * Distributes the minimum distance associated with a given vertex among all the target vertices
     * summed up with the edge's value.
     *
     * @param <K>
     */
    @SuppressWarnings("serial")
    public static final class MinDistanceMessenger<K extends Comparable<K> & Serializable>
            extends MessagingFunction<K, Double, Double, Double> {

        @Override
        public void sendMessages(K vertexKey, Double newDistance) throws Exception {
            for (OutgoingEdge<K, Double> edge : getOutgoingEdges()) {
                sendMessageTo(edge.target(), newDistance + edge.edgeValue());
            }
        }
    }

    private static final class SelectVertex<K extends Comparable<K> & Serializable>
            implements FilterFunction<Vertex<K, Double>> {
        private K id;

        public SelectVertex(K id) {
            this.id = id;
        }

        @Override
        public boolean filter(Vertex<K, Double> vertex) throws Exception {
            return vertex.getId().equals(id);
        }
    }

    private static final class InitSrcVertex<K extends Comparable<K> & Serializable>
            implements MapFunction<Tuple2<Vertex<K, Double>, Vertex<K,Double>>, Vertex<K,Double>> {

        @Override
        public Vertex<K, Double> map(Tuple2<Vertex<K, Double>, Vertex<K, Double>> vertexVertexTuple2) throws Exception {
            if(vertexVertexTuple2.f0.f0.equals(vertexVertexTuple2.f1.f0)) {
                return new Vertex<>(vertexVertexTuple2.f0.f0, 0.0);
            } else {
                return new Vertex<>(vertexVertexTuple2.f1.f0, Double.MAX_VALUE);
            }
        }
    }

}