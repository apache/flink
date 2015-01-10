package flink.graphs.example;

import java.util.Collection;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.NullValue;

import flink.graphs.Graph;
import flink.graphs.example.utils.ExampleUtils;

/**
 * 
 * A simple example to illustrate the basic functionality of the graph-api.
 * The program creates a random graph and computes and prints
 * the following metrics:
 * - number of vertices
 * - number of edges
 * - average node degree
 * - the vertex ids with the max/min in- and out-degrees
 *
 */
public class GraphMetrics implements ProgramDescription {

	static final int NUM_VERTICES = 100;
	static final long SEED = 9876;
	

	@Override
	public String getDescription() {
		return "Graph Metrics Example";
	}

	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		/** create a random graph **/
		Graph<Long, NullValue, NullValue> graph = Graph.create(ExampleUtils
				.getRandomEdges(env, NUM_VERTICES), env);
		
		/** get the number of vertices **/
		DataSet<Integer> numVertices = graph.numberOfVertices();
		
		/** get the number of edges **/
		DataSet<Integer> numEdges = graph.numberOfEdges();
		
		/** compute the average node degree **/
		DataSet<Tuple2<Long, Long>> verticesWithDegrees = graph.getDegrees();

		DataSet<Double> avgNodeDegree = verticesWithDegrees
				.aggregate(Aggregations.SUM, 1).map(new AvgNodeDegreeMapper())
				.withBroadcastSet(numVertices, "numberOfVertices");
		
		/** find the vertex with the maximum in-degree **/
		DataSet<Long> maxInDegreeVertex = graph.inDegrees().maxBy(1).map(new ProjectVertexId());

		/** find the vertex with the minimum in-degree **/
		DataSet<Long> minInDegreeVertex = graph.inDegrees().minBy(1).map(new ProjectVertexId());

		/** find the vertex with the maximum out-degree **/
		DataSet<Long> maxOutDegreeVertex = graph.outDegrees().maxBy(1).map(new ProjectVertexId());

		/** find the vertex with the minimum out-degree **/
		DataSet<Long> minOutDegreeVertex = graph.outDegrees().minBy(1).map(new ProjectVertexId());
		
		/** print the results **/
		ExampleUtils.printResult(numVertices, "Total number of vertices", env);
		ExampleUtils.printResult(numEdges, "Total number of edges", env);
		ExampleUtils.printResult(avgNodeDegree, "Average node degree", env);
		ExampleUtils.printResult(maxInDegreeVertex, "Vertex with Max in-degree", env);
		ExampleUtils.printResult(minInDegreeVertex, "Vertex with Min in-degree", env);
		ExampleUtils.printResult(maxOutDegreeVertex, "Vertex with Max out-degree", env);
		ExampleUtils.printResult(minOutDegreeVertex, "Vertex with Min out-degree", env);

		env.execute();
	}
	
	@SuppressWarnings("serial")
	private static final class AvgNodeDegreeMapper extends RichMapFunction<Tuple2<Long, Long>, Double> {

		private int numberOfVertices;
		
		@Override
		public void open(Configuration parameters) throws Exception {
			Collection<Integer> bCastSet = getRuntimeContext()
					.getBroadcastVariable("numberOfVertices");
			numberOfVertices = bCastSet.iterator().next();
		}
		
		public Double map(Tuple2<Long, Long> sumTuple) {
			return (double) (sumTuple.f1 / numberOfVertices) ;
		}
	}

	@SuppressWarnings("serial")
	private static final class ProjectVertexId implements MapFunction<Tuple2<Long,Long>, Long> {
		public Long map(Tuple2<Long, Long> value) { return value.f0; }
	}
}
