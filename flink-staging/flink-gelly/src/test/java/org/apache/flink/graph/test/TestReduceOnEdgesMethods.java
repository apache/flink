package flink.graphs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestReduceOnEdgesMethods extends JavaProgramTestBase {

	private static int NUM_PROGRAMS = 6;
	
	private int curProgId = config.getInteger("ProgramId", -1);
	private String resultPath;
	private String expectedResult;
	
	public TestReduceOnEdgesMethods(Configuration config) {
		super(config);
	}
	
	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void testProgram() throws Exception {
		expectedResult = GraphProgs.runProgram(curProgId, resultPath);
	}
	
	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(expectedResult, resultPath);
	}
	
	@Parameters
	public static Collection<Object[]> getConfigurations() throws FileNotFoundException, IOException {

		LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

		for(int i=1; i <= NUM_PROGRAMS; i++) {
			Configuration config = new Configuration();
			config.setInteger("ProgramId", i);
			tConfigs.add(config);
		}
		
		return toParameterList(tConfigs);
	}
	
	private static class GraphProgs {
	
		@SuppressWarnings("serial")
		public static String runProgram(int progId, String resultPath) throws Exception {
			
			switch(progId) {
			case 1: {
				/*
				 * Get the lowest-weight out-neighbor
				 * for each vertex
		         */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env), 
						TestGraphUtils.getLongLongEdgeData(env), env);

				DataSet<Tuple2<Long, Long>> verticesWithLowestOutNeighbor = 
						graph.reduceOnEdges(new EdgesFunctionWithVertexValue<Long, Long, Long, Long>() {

					public Tuple2<Long, Long> iterateEdges(
							Vertex<Long, Long> v,
							Iterable<Edge<Long, Long>> edges) {
						
						long weight = Long.MAX_VALUE;
						long minNeighorId = 0;
						
						for (Edge<Long, Long> edge: edges) {
							if (edge.getValue() < weight) {
								weight = edge.getValue();
								minNeighorId = edge.getTarget();
							}
						}
						return new Tuple2<Long, Long>(v.getId(), minNeighorId);
					}
				}, EdgeDirection.OUT);
				verticesWithLowestOutNeighbor.writeAsCsv(resultPath);
				env.execute();
				return "1,2\n" +
						"2,3\n" + 
						"3,4\n" +
						"4,5\n" + 
						"5,1\n";
			}
			case 2: {
				/*
				 * Get the lowest-weight in-neighbor
				 * for each vertex
		         */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env), 
						TestGraphUtils.getLongLongEdgeData(env), env);

				DataSet<Tuple2<Long, Long>> verticesWithLowestOutNeighbor = 
						graph.reduceOnEdges(new EdgesFunctionWithVertexValue<Long, Long, Long, Long>() {

					public Tuple2<Long, Long> iterateEdges(
							Vertex<Long, Long> v,
							Iterable<Edge<Long, Long>> edges) {
						
						long weight = Long.MAX_VALUE;
						long minNeighorId = 0;
						
						for (Edge<Long, Long> edge: edges) {
							if (edge.getValue() < weight) {
								weight = edge.getValue();
								minNeighorId = edge.getSource();
							}
						}
						return new Tuple2<Long, Long>(v.getId(), minNeighorId);
					}
				}, EdgeDirection.IN);
				verticesWithLowestOutNeighbor.writeAsCsv(resultPath);
				env.execute();
				return "1,5\n" +
						"2,1\n" + 
						"3,1\n" +
						"4,3\n" + 
						"5,3\n";
			}
			case 3: {
				/*
				 * Get the maximum weight among all edges
				 * of a vertex
		         */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env), 
						TestGraphUtils.getLongLongEdgeData(env), env);

				DataSet<Tuple2<Long, Long>> verticesWithMaxEdgeWeight = 
						graph.reduceOnEdges(new EdgesFunctionWithVertexValue<Long, Long, Long, Long>() {

					public Tuple2<Long, Long> iterateEdges(Vertex<Long, Long> v,
							Iterable<Edge<Long, Long>> edges) {
						
						long weight = Long.MIN_VALUE;

						for (Edge<Long, Long> edge: edges) {
							if (edge.getValue() > weight) {
								weight = edge.getValue();
							}
						}
						return new Tuple2<Long, Long>(v.getId(), weight);
					}
				}, EdgeDirection.ALL);
				verticesWithMaxEdgeWeight.writeAsCsv(resultPath);
				env.execute();
				return "1,51\n" +
						"2,23\n" + 
						"3,35\n" +
						"4,45\n" + 
						"5,51\n";
			}
			case 4: {
				/*
				 * Get the lowest-weight out-neighbor
				 * for each vertex
		         */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env), 
						TestGraphUtils.getLongLongEdgeData(env), env);

				DataSet<Tuple2<Long, Long>> verticesWithLowestOutNeighbor = 
						graph.reduceOnEdges(new EdgesFunction<Long, Long, Long>() {

					public Tuple2<Long, Long> iterateEdges(Iterable<Tuple2<Long, Edge<Long, Long>>> edges) {

						long weight = Long.MAX_VALUE;
						long minNeighorId = 0;
						long vertexId = -1;
						long i=0;

						for (Tuple2<Long, Edge<Long, Long>> edge: edges) {
							if (edge.f1.getValue() < weight) {
								weight = edge.f1.getValue();
								minNeighorId = edge.f1.getTarget();
							}
							if (i==0) {
								vertexId = edge.f0;
							} i++;
						}
						return new Tuple2<Long, Long>(vertexId, minNeighorId);
					}
				}, EdgeDirection.OUT);
				verticesWithLowestOutNeighbor.writeAsCsv(resultPath);
				env.execute();
				return "1,2\n" +
						"2,3\n" + 
						"3,4\n" +
						"4,5\n" + 
						"5,1\n";
			}
			case 5: {
				/*
				 * Get the lowest-weight in-neighbor
				 * for each vertex
		         */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env), 
						TestGraphUtils.getLongLongEdgeData(env), env);

				DataSet<Tuple2<Long, Long>> verticesWithLowestOutNeighbor = 
						graph.reduceOnEdges(new EdgesFunction<Long, Long, Long>() {

					public Tuple2<Long, Long> iterateEdges(Iterable<Tuple2<Long, Edge<Long, Long>>> edges) {
						
						long weight = Long.MAX_VALUE;
						long minNeighorId = 0;
						long vertexId = -1;
						long i=0;

						for (Tuple2<Long, Edge<Long, Long>> edge: edges) {
							if (edge.f1.getValue() < weight) {
								weight = edge.f1.getValue();
								minNeighorId = edge.f1.getSource();
							}
							if (i==0) {
								vertexId = edge.f0;
							} i++;
						}
						return new Tuple2<Long, Long>(vertexId, minNeighorId);
					}
				}, EdgeDirection.IN);
				verticesWithLowestOutNeighbor.writeAsCsv(resultPath);
				env.execute();
				return "1,5\n" +
						"2,1\n" + 
						"3,1\n" +
						"4,3\n" + 
						"5,3\n";
			}
			case 6: {
				/*
				 * Get the maximum weight among all edges
				 * of a vertex
		         */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env), 
						TestGraphUtils.getLongLongEdgeData(env), env);

				DataSet<Tuple2<Long, Long>> verticesWithMaxEdgeWeight = 
						graph.reduceOnEdges(new EdgesFunction<Long, Long, Long>() {

					public Tuple2<Long, Long> iterateEdges(Iterable<Tuple2<Long, Edge<Long, Long>>> edges) {
						
						long weight = Long.MIN_VALUE;
						long vertexId = -1;
						long i=0;

						for (Tuple2<Long, Edge<Long, Long>> edge: edges) {
							if (edge.f1.getValue() > weight) {
								weight = edge.f1.getValue();
							}
							if (i==0) {
								vertexId = edge.f0;
							} i++;
						}
						return new Tuple2<Long, Long>(vertexId, weight);
					}
				}, EdgeDirection.ALL);
				verticesWithMaxEdgeWeight.writeAsCsv(resultPath);
				env.execute();
				return "1,51\n" +
						"2,23\n" + 
						"3,35\n" +
						"4,45\n" + 
						"5,51\n";
			}
			default: 
				throw new IllegalArgumentException("Invalid program id");
			}
		}
	}
}
