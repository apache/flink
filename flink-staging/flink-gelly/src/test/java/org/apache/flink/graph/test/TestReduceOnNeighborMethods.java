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
public class TestReduceOnNeighborMethods extends JavaProgramTestBase {

	private static int NUM_PROGRAMS = 3;
	
	private int curProgId = config.getInteger("ProgramId", -1);
	private String resultPath;
	private String expectedResult;
	
	public TestReduceOnNeighborMethods(Configuration config) {
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
				 * Get the sum of out-neighbor values
				 * for each vertex
		         */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env), 
						TestGraphUtils.getLongLongEdgeData(env), env);

				DataSet<Tuple2<Long, Long>> verticesWithSumOfOutNeighborValues = 
						graph.reduceOnNeighbors(new NeighborsFunctionWithVertexValue<Long, Long, Long, Long>() {
							public Tuple2<Long, Long> iterateNeighbors(Vertex<Long, Long> vertex,
									Iterable<Tuple2<Edge<Long, Long>, Vertex<Long, Long>>> neighbors) {
								long sum = 0;
								for (Tuple2<Edge<Long, Long>, Vertex<Long, Long>> neighbor : neighbors) {
									sum += neighbor.f1.getValue();
								}
								return new Tuple2<Long, Long>(vertex.getId(), sum);
							}
						}, EdgeDirection.OUT);

				verticesWithSumOfOutNeighborValues.writeAsCsv(resultPath);
				env.execute();
				return "1,5\n" +
						"2,3\n" + 
						"3,9\n" +
						"4,5\n" + 
						"5,1\n";
			}
			case 2: {
				/*
				 * Get the sum of in-neighbor values
				 * time edge weights for each vertex
		         */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env), 
						TestGraphUtils.getLongLongEdgeData(env), env);

				DataSet<Tuple2<Long, Long>> verticesWithSum = 
						graph.reduceOnNeighbors(new NeighborsFunctionWithVertexValue<Long, Long, Long, Long>() {
							public Tuple2<Long, Long> iterateNeighbors(Vertex<Long, Long> vertex,
									Iterable<Tuple2<Edge<Long, Long>, Vertex<Long, Long>>> neighbors) {
								long sum = 0;
								for (Tuple2<Edge<Long, Long>, Vertex<Long, Long>> neighbor : neighbors) {
									sum += neighbor.f0.getValue() * neighbor.f1.getValue();
								}
								return new Tuple2<Long, Long>(vertex.getId(), sum);
							}
						}, EdgeDirection.IN);		

				verticesWithSum.writeAsCsv(resultPath);
				env.execute();
				return "1,255\n" +
						"2,12\n" + 
						"3,59\n" +
						"4,102\n" + 
						"5,285\n";
			}
			case 3: {
				/*
				 * Get the sum of all neighbor values
				 * for each vertex
		         */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env), 
						TestGraphUtils.getLongLongEdgeData(env), env);

				DataSet<Tuple2<Long, Long>> verticesWithSumOfOutNeighborValues = 
						graph.reduceOnNeighbors(new NeighborsFunctionWithVertexValue<Long, Long, Long, Long>() {
							public Tuple2<Long, Long> iterateNeighbors(Vertex<Long, Long> vertex,
									Iterable<Tuple2<Edge<Long, Long>, Vertex<Long, Long>>> neighbors) {
								long sum = 0;
								for (Tuple2<Edge<Long, Long>, Vertex<Long, Long>> neighbor : neighbors) {
									sum += neighbor.f1.getValue();
								}
								return new Tuple2<Long, Long>(vertex.getId(), sum);
							}
						}, EdgeDirection.ALL);

				verticesWithSumOfOutNeighborValues.writeAsCsv(resultPath);
				env.execute();
				return "1,10\n" +
						"2,4\n" + 
						"3,12\n" +
						"4,8\n" + 
						"5,8\n";
			}
			default: 
				throw new IllegalArgumentException("Invalid program id");
			}
		}
	}
}