package flink.graphs.example;


import flink.graphs.*;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.spargel.java.MessageIterator;
import org.apache.flink.spargel.java.MessagingFunction;
import org.apache.flink.spargel.java.OutgoingEdge;
import org.apache.flink.spargel.java.VertexUpdateFunction;
import org.apache.flink.util.Collector;


public class PageRank implements ProgramDescription {

    private static final double BETA = 0.85;


    public static void main (String [] args) throws Exception {

        final int numVertices = 10;

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Vertex<Long, Double>> pages = env.generateSequence(1, numVertices)
                .map(new MapFunction<Long, Vertex<Long, Double>>() {
                         @Override
                         public Vertex<Long, Double> map(Long l) throws Exception {
                             return new Vertex<Long, Double>(l, 1.0 / numVertices);
                         }
                     });

        DataSet<Edge<Long,Double>> links = env.generateSequence(1, numVertices)
                .flatMap(new FlatMapFunction<Long, Edge<Long, Double>>() {
                    @Override
                    public void flatMap(Long key, Collector<Edge<Long, Double>> out) throws Exception {
                        int numOutEdges = (int) (Math.random() * (numVertices / 2));
                        for (int i = 0; i < numOutEdges; i++) {
                            long target = (long) (Math.random() * numVertices) + 1;
                            out.collect(new Edge<Long, Double>(key, target, 1.0/numOutEdges));
                        }
                    }
                });

        Graph<Long, Double, Double> network = new Graph<Long, Double, Double>(pages, links, env);

        DataSet<Vertex<Long,Double>> verticesWithRanks = GraphLib.pageRank(network, numVertices, BETA, 60).getVertices();

        verticesWithRanks.print();

        env.execute();
    }

    @Override
    public String getDescription() {
        return "PageRank";
    }
}
