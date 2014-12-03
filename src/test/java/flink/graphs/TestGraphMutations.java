package flink.graphs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestGraphMutations extends JavaProgramTestBase {

	private static int NUM_PROGRAMS = 9;
	
	private int curProgId = config.getInteger("ProgramId", -1);
	private String resultPath;
	private String expectedResult;
	
	public TestGraphMutations(Configuration config) {
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
		
		public static String runProgram(int progId, String resultPath) throws Exception {
			
			switch(progId) {
			case 1: {
				/*
				 * Test addVertex() -- simple case
				 */	
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
						TestGraphUtils.getLongLongEdgeData(env), env);
				
				List<Edge<Long, Long>> edges = new ArrayList<Edge<Long, Long>>();
				edges.add(new Edge<Long, Long>(6L, 1L, 61L));
				graph = graph.addVertex(new Vertex<Long, Long>(6L, 6L), edges);
				graph.getEdges().writeAsCsv(resultPath);
				env.execute();
				
				return "1,2,12\n" +
					"1,3,13\n" +
					"2,3,23\n" +
					"3,4,34\n" +
					"3,5,35\n" +
					"4,5,45\n" +
					"5,1,51\n" +
					"6,1,61\n";
				
			}
			case 2: {
				/*
				 * Test addVertex() -- add an existing vertex
				 */	
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
						TestGraphUtils.getLongLongEdgeData(env), env);
				
				List<Edge<Long, Long>> edges = new ArrayList<Edge<Long, Long>>();
				edges.add(new Edge<Long, Long>(1L, 5L, 15L));
				graph = graph.addVertex(new Vertex<Long, Long>(1L, 1L), edges);
				graph.getEdges().writeAsCsv(resultPath);
				env.execute();

				return "1,2,12\n" +
					"1,3,13\n" +
					"1,5,15\n" +
					"2,3,23\n" +
					"3,4,34\n" +
					"3,5,35\n" +
					"4,5,45\n" +
					"5,1,51\n";
				
			}
			case 3: {
				/*
				 * Test addVertex() -- add vertex with empty edge set
				 */	
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
						TestGraphUtils.getLongLongEdgeData(env), env);
				List<Edge<Long, Long>> edges = new ArrayList<Edge<Long, Long>>();
				graph = graph.addVertex(new Vertex<Long, Long>(6L, 6L), edges);
				graph.getVertices().writeAsCsv(resultPath);
				env.execute();

				return "1,1\n" +
					"2,2\n" +
					"3,3\n" +
					"4,4\n" +
					"5,5\n" +
					"6,6\n";
				
			}
			case 4: {
				/*
				 * Test removeVertex() -- simple case
				 */	
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
						TestGraphUtils.getLongLongEdgeData(env), env);
				graph = graph.removeVertex(new Vertex<Long, Long>(5L, 5L));
				graph.getEdges().writeAsCsv(resultPath);
				env.execute();

				return "1,2,12\n" +
					"1,3,13\n" +
					"2,3,23\n" +
					"3,4,34\n";
				
			}
			case 5: {
				/*
				 * Test removeVertex() -- remove an invalid vertex
				 */	
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
						TestGraphUtils.getLongLongEdgeData(env), env);
				graph = graph.removeVertex(new Vertex<Long, Long>(6L, 6L));
				graph.getEdges().writeAsCsv(resultPath);
				env.execute();

				return "1,2,12\n" +
					"1,3,13\n" +
					"2,3,23\n" +
					"3,4,34\n" +
					"3,5,35\n" +
					"4,5,45\n" +
					"5,1,51\n";
			}
			case 6: {
				/*
				 * Test addEdge() -- simple case
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
						TestGraphUtils.getLongLongEdgeData(env), env);
				graph = graph.addEdge(new Vertex<Long, Long>(6L, 6L), new Vertex<Long, Long>(1L, 1L),
						61L);
				graph.getEdges().writeAsCsv(resultPath);
				env.execute();

				return "1,2,12\n" +
					"1,3,13\n" +
					"2,3,23\n" +
					"3,4,34\n" +
					"3,5,35\n" +
					"4,5,45\n" +
					"5,1,51\n" +
					"6,1,61\n";	
			}
			case 7: {
				/*
				 * Test addEdge() -- add already existing edge
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
						TestGraphUtils.getLongLongEdgeData(env), env);
				graph = graph.addEdge(new Vertex<Long, Long>(1L, 1L), new Vertex<Long, Long>(2L, 2L),
						12L);
				graph.getEdges().writeAsCsv(resultPath);
				env.execute();

				return "1,2,12\n" +
					"1,2,12\n" +
					"1,3,13\n" +
					"2,3,23\n" +
					"3,4,34\n" +
					"3,5,35\n" +
					"4,5,45\n" +
					"5,1,51\n";	
			}
			case 8: {
				/*
				 * Test removeEdge() -- simple case
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
						TestGraphUtils.getLongLongEdgeData(env), env);
				graph = graph.removeEdge(new Edge<Long, Long>(5L, 1L, 51L));
				graph.getEdges().writeAsCsv(resultPath);
				env.execute();

				return "1,2,12\n" +
					"1,3,13\n" +
					"2,3,23\n" +
					"3,4,34\n" +
					"3,5,35\n" +
					"4,5,45\n";
				
			}
			case 9: {
				/*
				 * Test removeEdge() -- invalid edge
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
						TestGraphUtils.getLongLongEdgeData(env), env);
				graph = graph.removeEdge(new Edge<Long, Long>(6L, 1L, 61L));
				graph.getEdges().writeAsCsv(resultPath);
				env.execute();
				
				return "1,2,12\n" +
					"1,3,13\n" +
					"2,3,23\n" +
					"3,4,34\n" +
					"3,5,35\n" +
					"4,5,45\n" +
					"5,1,51\n";
				
			}
			default: 
				throw new IllegalArgumentException("Invalid program id");
			}
		}
	}
	
}
