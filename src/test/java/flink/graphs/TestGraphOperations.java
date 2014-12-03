package flink.graphs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestGraphOperations extends JavaProgramTestBase {

	private static int NUM_PROGRAMS = 10;

	private int curProgId = config.getInteger("ProgramId", -1);
	private String resultPath;
	private String expectedResult;

	public TestGraphOperations(Configuration config) {
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
				 * Test getUndirected()
				 */
					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

					Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
							TestGraphUtils.getLongLongEdgeData(env), env);

					graph.getUndirected().getEdges().writeAsCsv(resultPath);
					env.execute();
					return "1,2,12\n" + "2,1,12\n" +
							"1,3,13\n" + "3,1,13\n" +
							"2,3,23\n" + "3,2,23\n" +
							"3,4,34\n" + "4,3,34\n" +
							"3,5,35\n" + "5,3,35\n" +
							"4,5,45\n" + "5,4,45\n" +
							"5,1,51\n" + "1,5,51\n";
				}
				case 2: {
				/*
				 * Test reverse()
				 */
					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

					Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
							TestGraphUtils.getLongLongEdgeData(env), env);

					graph.reverse().getEdges().writeAsCsv(resultPath);
					env.execute();
					return "2,1,12\n" +
							"3,1,13\n" +
							"3,2,23\n" +
							"4,3,34\n" +
							"5,3,35\n" +
							"5,4,45\n" +
							"1,5,51\n";
				}
				case 3: {
				/*
				 * Test subgraph:
				 */
					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

					Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
							TestGraphUtils.getLongLongEdgeData(env), env);
					graph.subgraph(new FilterFunction<Long>() {
									   public boolean filter(Long value) throws Exception {
										   return (value > 2);
									   }
								   },
							new FilterFunction<Long>() {
								public boolean filter(Long value) throws Exception {
									return (value > 34);
								}
							}).getEdges().writeAsCsv(resultPath);

					env.execute();
					return "3,5,35\n" +
							"4,5,45\n";
				}
				case 4: {
				/*
				 * Test filterOnVertices:
				 */
					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

					Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
							TestGraphUtils.getLongLongEdgeData(env), env);
					graph.filterOnVertices(new FilterFunction<Long>() {
						public boolean filter(Long value) throws Exception {
							return (value > 2);
						}
					}).getEdges().writeAsCsv(resultPath);

					env.execute();
					return  "3,4,34\n" +
							"3,5,35\n" +
							"4,5,45\n";
				}
				case 5: {
				/*
				 * Test filterOnEdges:
				 */
					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

					Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
							TestGraphUtils.getLongLongEdgeData(env), env);
					graph.filterOnEdges(new FilterFunction<Long>() {
						public boolean filter(Long value) throws Exception {
							return (value > 34);
						}
					}).getEdges().writeAsCsv(resultPath);

					env.execute();
					return "3,5,35\n" +
							"4,5,45\n" +
							"5,1,51\n";
				}
				case 6: {
				/*
				 * Test numberOfVertices()
				 */
					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

					Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
							TestGraphUtils.getLongLongEdgeData(env), env);
					graph.numberOfVertices().writeAsText(resultPath);

					env.execute();
					return "5";
				}
				case 7: {
				/*
				 * Test numberOfEdges()
				 */
					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

					Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
							TestGraphUtils.getLongLongEdgeData(env), env);
					graph.numberOfEdges().writeAsText(resultPath);

					env.execute();
					return "7";
				}
				case 8: {
				/*
				 * Test getVertexIds()
				 */
					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

					Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
							TestGraphUtils.getLongLongEdgeData(env), env);
					graph.getVertexIds().writeAsText(resultPath);

					env.execute();
					return "1\n2\n3\n4\n5\n";
				}
				case 9: {
				/*
				 * Test getEdgeIds()
				 */
					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

					Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
							TestGraphUtils.getLongLongEdgeData(env), env);
					graph.getEdgeIds().writeAsCsv(resultPath);

					env.execute();
					return "1,2\n" + "1,3\n" +
							"2,3\n" + "3,4\n" +
							"3,5\n" + "4,5\n" +
							"5,1\n";
				}
				case 10: {
				/*
				 * Test union()
				 */

					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

					Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
							TestGraphUtils.getLongLongEdgeData(env), env);

					List<Vertex<Long, Long>> vertices = new ArrayList<Vertex<Long, Long>>();
					List<Edge<Long, Long>> edges = new ArrayList<Edge<Long, Long>>();

					vertices.add(new Vertex<Long, Long>(6L, 6L));
					edges.add(new Edge<Long, Long>(6L, 1L, 61L));

					graph = graph.union(graph.fromCollection(vertices, edges));

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
				default:
					throw new IllegalArgumentException("Invalid program id");
			}
		}
	}

}
