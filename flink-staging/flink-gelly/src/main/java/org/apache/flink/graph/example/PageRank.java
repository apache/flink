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

        network.runVertexCentricIteration(new VertexRankUpdater(numVertices, BETA), new RankMessenger(), 60)
                .getVertices()
                .print();

        env.execute();
    }

    /**
     * Function that updates the rank of a vertex by summing up the partial ranks from all incoming messages
     * and then applying the dampening formula.
     */
    public static final class VertexRankUpdater extends VertexUpdateFunction<Long, Double, Double> {

        private final long numVertices;
        private final double beta;

        public VertexRankUpdater(long numVertices, double beta) {
            this.numVertices = numVertices;
            this.beta = beta;
        }

        @Override
        public void updateVertex(Long vertexKey, Double vertexValue, MessageIterator<Double> inMessages) {
            double rankSum = 0.0;
            for (double msg : inMessages) {
                rankSum += msg;
            }

            // apply the dampening factor / random jump
            double newRank = (beta * rankSum) + (1-BETA)/numVertices;
            setNewVertexValue(newRank);
        }
    }

    /**
     * Distributes the rank of a vertex among all target vertices according to the transition probability,
     * which is associated with an edge as the edge value.
     */
    public static final class RankMessenger extends MessagingFunction<Long, Double, Double, Double> {

        @Override
        public void sendMessages(Long vertexId, Double newRank) {
            for (OutgoingEdge<Long, Double> edge : getOutgoingEdges()) {
                sendMessageTo(edge.target(), newRank * edge.edgeValue());
            }
        }
    }


    @Override
    public String getDescription() {
        return "PageRank";
    }
}
