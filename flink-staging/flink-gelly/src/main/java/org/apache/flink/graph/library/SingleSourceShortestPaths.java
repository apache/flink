package flink.graphs.library;

import flink.graphs.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class SingleSourceShortestPaths implements GraphAlgorithm<Long, Double, Double> {

    private final Long srcVertexId;
    private final Integer maxIterations;

    public SingleSourceShortestPaths(Long srcVertexId, Integer maxIterations) {
        this.srcVertexId = srcVertexId;
        this.maxIterations = maxIterations;
    }

    private Graph<Long, Double, Double> computeDeltaShortestPath(Graph<Long, Double, Double> graph) {
        DataSet<Vertex<Long, Double>> sourceVertex = graph.getVertices().filter(
                new SelectVertex(srcVertexId));
        //assign the initial distance as a value for each vertex
        DataSet<Vertex<Long, Double>> verticesWithInitialDistance = sourceVertex.cross(graph.getVertices())
                .map(new InitSrcVertex());

        //open a delta iteration
        DeltaIteration<Vertex<Long, Double>, Vertex<Long, Double>> iteration =
                verticesWithInitialDistance.iterateDelta(verticesWithInitialDistance, maxIterations, 0);

        //apply the step logic: join with the edges, choose the minimum distance and update if
        //the new distance proposed is smaller
        DataSet<Vertex<Long, Double>> changes = iteration.getWorkset()
                .join(graph.getEdges()).where(0).equalTo(0).with(new NeighborWithDistanceJoin())
                .groupBy(0).aggregate(Aggregations.MIN, 1)
                .join(iteration.getSolutionSet()).where(0).equalTo(0)
                .flatMap(new DistanceFilter());

        // close the delta iteration (delta and new workset are identical)
        DataSet<Vertex<Long, Double>> result = iteration.closeWith(changes, changes);

        return Graph.create(result, graph.getEdges(), ExecutionEnvironment.getExecutionEnvironment());
    }

    private static final class SelectVertex implements FilterFunction<Vertex<Long, Double>> {

        private Long id;

        public SelectVertex(Long id) {
            this.id = id;
        }

        @Override
        public boolean filter(Vertex<Long, Double> kvvVertex) throws Exception {
            return kvvVertex.getId().equals(id);
        }
    }

    private static final class InitSrcVertex
            implements MapFunction<Tuple2<Vertex<Long, Double>, Vertex<Long,Double>>, Vertex<Long,Double>> {

        @Override
        public Vertex<Long, Double> map(Tuple2<Vertex<Long, Double>, Vertex<Long, Double>> vertexVertexTuple2) throws Exception {
            if(vertexVertexTuple2.f0.f0.equals(vertexVertexTuple2.f1.f0)) {
                return new Vertex<>(vertexVertexTuple2.f0.f0, 0.0);
            } else {
                return new Vertex<>(vertexVertexTuple2.f1.f0, Double.MAX_VALUE);
            }
        }
    }

    public static final class NeighborWithDistanceJoin
            implements JoinFunction<Vertex<Long, Double>, Edge<Long, Double>, Vertex<Long, Double>> {

        @Override
        public Vertex<Long, Double> join(Vertex<Long, Double> kvvVertex, Edge<Long, Double> kevEdge) throws Exception {
            return new Vertex<>(kevEdge.f1, kvvVertex.f1 + kevEdge.f2);
        }
    }

    public static final class DistanceFilter
            implements FlatMapFunction<Tuple2<Vertex<Long, Double>, Vertex<Long, Double>>, Vertex<Long, Double>> {

        @Override
        public void flatMap(Tuple2<Vertex<Long, Double>, Vertex<Long, Double>> value,
                            Collector<Vertex<Long, Double>> collector) throws Exception {
            if (value.f0.f1 < value.f1.f1) {
                collector.collect(value.f0);
            }
        }
    }

    @Override
    public Graph<Long, Double, Double> run(Graph<Long, Double, Double> input) {

        return computeDeltaShortestPath(input);
    }

}
