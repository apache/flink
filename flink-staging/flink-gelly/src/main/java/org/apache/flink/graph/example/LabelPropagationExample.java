package flink.graphs.example;


import java.util.Random;

import flink.graphs.*;
import flink.graphs.library.LabelPropagation;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

public class LabelPropagationExample implements ProgramDescription {

    public static void main (String [] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Vertex<Long, Long>> vertices = getVertexDataSet(env);
        DataSet<Edge<Long, NullValue>> edges = getEdgeDataSet(env);

        Graph<Long, Long, NullValue> graph = new Graph<Long, Long, NullValue>(vertices, edges, env);

        DataSet<Vertex<Long, Long>> verticesWithCommunity =
                graph.run(new LabelPropagation<Long>(maxIterations)).getVertices();

        verticesWithCommunity.print();

        env.execute();
    }

    @Override
    public String getDescription() {
        return "Label Propagation Example";
    }

    private static long numVertices = 20;
    private static int maxIterations = 10;
    private static int numberOfLabels = 3;

    @SuppressWarnings("serial")
	private static DataSet<Vertex<Long, Long>> getVertexDataSet(ExecutionEnvironment env) {
            return env.generateSequence(1, numVertices)
                    .map(new MapFunction<Long, Vertex<Long, Long>>() {
                        public Vertex<Long, Long> map(Long l) throws Exception {
                        	Random randomGenerator = new Random();
                            return new Vertex<Long, Long>(l, (long) randomGenerator.nextInt((int) numberOfLabels));
                        }
                    });
    }

    @SuppressWarnings("serial")
    private static DataSet<Edge<Long, NullValue>> getEdgeDataSet(ExecutionEnvironment env) {
            return env.generateSequence(1, numVertices)
                    .flatMap(new FlatMapFunction<Long, Edge<Long, NullValue>>() {
                        @Override
                        public void flatMap(Long key, Collector<Edge<Long, NullValue>> out) {
                            int numOutEdges = (int) (Math.random() * (numVertices / 2));
                            for (int i = 0; i < numOutEdges; i++) {
                                long target = (long) (Math.random() * numVertices) + 1;
                                out.collect(new Edge<Long, NullValue>(key, target, NullValue.getInstance()));
                            }
                        }
                    });
    }
}