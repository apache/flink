package flink.graphs.example;

import flink.graphs.*;
import flink.graphs.library.LabelPropagation;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

/**
 * This example uses the label propagation algorithm to detect communities by propagating labels.
 * Initially, each vertex is assigned its id as its label.
 * The vertices iteratively propagate their labels to their neighbors and adopt the most frequent label
 * among their neighbors.
 * The algorithm converges when no vertex changes value or the maximum number of iterations have been reached.
 */
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

    private static long numVertices = 100;
    private static int maxIterations = 20;

	@SuppressWarnings("serial")
	private static DataSet<Vertex<Long, Long>> getVertexDataSet(ExecutionEnvironment env) {
            return env.generateSequence(1, numVertices)
                    .map(new MapFunction<Long, Vertex<Long, Long>>() {
                        public Vertex<Long, Long> map(Long l) throws Exception {
                            return new Vertex<Long, Long>(l, l);
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