package flink.graphs.example;


import flink.graphs.*;
import flink.graphs.library.PageRank;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class PageRankExample implements ProgramDescription {

    @SuppressWarnings("serial")
	public static void main (String [] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Vertex<Long,Double>> pages = getPagesDataSet(env);

        DataSet<Edge<Long,Double>> links = getLinksDataSet(env);

        Graph<Long, Double, Double> network = new Graph<Long, Double, Double>(pages, links, env);
        
        DataSet<Tuple2<Long, Long>> vertexOutDegrees = network.outDegrees();
        
        // assign the transition probabilities as the edge weights
        Graph<Long, Double, Double> networkWithWeights = network.joinWithEdgesOnSource(vertexOutDegrees, 
        		new MapFunction<Tuple2<Double, Long>, Double>() {
					public Double map(Tuple2<Double, Long> value) {
						return value.f0 / value.f1;
					}
				});

        DataSet<Vertex<Long,Double>> pageRanks =
        		networkWithWeights.run(new PageRank<Long>(numPages, DAMPENING_FACTOR, maxIterations)).getVertices();

        pageRanks.print();

        env.execute();
    }

    @Override
    public String getDescription() {
        return "PageRank";
    }

    private static final double DAMPENING_FACTOR = 0.85;
    private static long numPages = 10;
    private static int maxIterations = 10;

    @SuppressWarnings("serial")
	private static DataSet<Vertex<Long,Double>> getPagesDataSet(ExecutionEnvironment env) {
            return env.generateSequence(1, numPages)
                    .map(new MapFunction<Long, Vertex<Long, Double>>() {
                        @Override
                        public Vertex<Long, Double> map(Long l) throws Exception {
                            return new Vertex<Long, Double>(l, 1.0 / numPages);
                        }
                    });

    }

    @SuppressWarnings("serial")
    private static DataSet<Edge<Long, Double>> getLinksDataSet(ExecutionEnvironment env) {
            return env.generateSequence(1, numPages)
                    .flatMap(new FlatMapFunction<Long, Edge<Long, Double>>() {
                        @Override
                        public void flatMap(Long key, Collector<Edge<Long, Double>> out) throws Exception {
                            int numOutEdges = (int) (Math.random() * (numPages / 2));
                            for (int i = 0; i < numOutEdges; i++) {
                                long target = (long) (Math.random() * numPages) + 1;
                                out.collect(new Edge<Long, Double>(key, target, 1.0));
                            }
                        }
                    });
    }
}
