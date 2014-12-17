package flink.graphs.example;

import flink.graphs.Edge;
import flink.graphs.Graph;
import flink.graphs.Vertex;
import flink.graphs.library.PageRank;
import flink.graphs.library.SingleSourceShortestPaths;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class SingleSourceShortestPathsExample implements ProgramDescription {

    private static int maxIterations = 5;

    public static void main (String [] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Vertex<Long,Double>> vertices = getLongDoubleVertexData(env);

        DataSet<Edge<Long,Double>> edges = getLongDoubleEdgeData(env);

        Graph<Long, Double, Double> graph = Graph.create(vertices, edges, env);

        Long srcVertexId = 1L;

        DataSet<Vertex<Long,Double>> singleSourceShortestPaths =
                graph.run(new SingleSourceShortestPaths(srcVertexId, maxIterations)).getVertices();

        singleSourceShortestPaths.print();

        env.execute();
    }

    @Override
    public String getDescription() {
        return "Single Source Shortest Paths";
    }

    public static final DataSet<Vertex<Long, Double>> getLongDoubleVertexData(
            ExecutionEnvironment env) {
        List<Vertex<Long, Double>> vertices = new ArrayList<Vertex<Long, Double>>();
        vertices.add(new Vertex(1L, 1.0));
        vertices.add(new Vertex(2L, 2.0));
        vertices.add(new Vertex(3L, 3.0));
        vertices.add(new Vertex(4L, 4.0));
        vertices.add(new Vertex(5L, 5.0));

        return env.fromCollection(vertices);
    }

    public static final DataSet<Edge<Long, Double>> getLongDoubleEdgeData(
            ExecutionEnvironment env) {
        List<Edge<Long, Double>> edges = new ArrayList<Edge<Long, Double>>();
        edges.add(new Edge(1L, 2L, 12.0));
        edges.add(new Edge(1L, 3L, 13.0));
        edges.add(new Edge(2L, 3L, 23.0));
        edges.add(new Edge(3L, 4L, 34.0));
        edges.add(new Edge(3L, 5L, 35.0));
        edges.add(new Edge(4L, 5L, 45.0));
        edges.add(new Edge(5L, 1L, 51.0));

        return env.fromCollection(edges);
    }

}
