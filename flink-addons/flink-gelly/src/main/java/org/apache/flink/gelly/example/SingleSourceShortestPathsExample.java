package org.apache.flink.gelly.example;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.gelly.Edge;
import org.apache.flink.gelly.Graph;
import org.apache.flink.gelly.Vertex;
import org.apache.flink.gelly.example.utils.ExampleUtils;
import org.apache.flink.gelly.library.SingleSourceShortestPaths;

public class SingleSourceShortestPathsExample implements ProgramDescription {

    private static int maxIterations = 5;

    public static void main (String [] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Vertex<Long, Double>> vertices = ExampleUtils.getLongDoubleVertexData(env);

        DataSet<Edge<Long, Double>> edges = ExampleUtils.getLongDoubleEdgeData(env);

        Long srcVertexId = 1L;

        Graph<Long, Double, Double> graph = Graph.fromDataSet(vertices, edges, env);

        DataSet<Vertex<Long,Double>> singleSourceShortestPaths =
                graph.run(new SingleSourceShortestPaths<Long>(srcVertexId, maxIterations)).getVertices();

        singleSourceShortestPaths.print();

        env.execute();
    }

    @Override
    public String getDescription() {
        return "Single Source Shortest Paths";
    }
}
