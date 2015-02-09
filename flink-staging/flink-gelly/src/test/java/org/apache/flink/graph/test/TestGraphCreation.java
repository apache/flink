package flink.graphs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.types.NullValue;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import flink.graphs.TestGraphUtils.DummyCustomParameterizedType;
import flink.graphs.validation.InvalidVertexIdsValidator;

@RunWith(Parameterized.class)
public class TestGraphCreation extends JavaProgramTestBase {

	private static int NUM_PROGRAMS = 5;

	private int curProgId = config.getInteger("ProgramId", -1);
	private String resultPath;
	private String expectedResult;

	public TestGraphCreation(Configuration config) {
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
				 * Test create() with edge dataset and no vertex values
		         */
					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
					Graph<Long, NullValue, Long> graph = Graph.create(TestGraphUtils.getLongLongEdgeData(env), env);

					graph.getVertices().writeAsCsv(resultPath);
					env.execute();
					return "1,(null)\n" +
							"2,(null)\n" +
							"3,(null)\n" +
							"4,(null)\n" +
							"5,(null)\n";
				}
				case 2: {
				/*
				 * Test create() with edge dataset and a mapper that assigns the id as value
		         */
					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
					Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongEdgeData(env),
							new MapFunction<Long, Long>() {
								public Long map(Long vertexId) {
									return vertexId;
								}
							}, env);

					graph.getVertices().writeAsCsv(resultPath);
					env.execute();
					return "1,1\n" +
							"2,2\n" +
							"3,3\n" +
							"4,4\n" +
							"5,5\n";
				}
				case 3: {
				/*
				 * Test create() with edge dataset and a mapper that assigns a parametrized custom vertex value
				 */
					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
					Graph<Long, DummyCustomParameterizedType<Double>, Long> graph = Graph.create(
							TestGraphUtils.getLongLongEdgeData(env),
							new MapFunction<Long, DummyCustomParameterizedType<Double>>() {

								DummyCustomParameterizedType<Double> dummyValue =
										new DummyCustomParameterizedType<Double>();

								public DummyCustomParameterizedType<Double> map(Long vertexId) {
									dummyValue.setIntField(vertexId.intValue()-1);
									dummyValue.setTField(vertexId*2.0);
									return dummyValue;
								}
							}, env);

					graph.getVertices().writeAsCsv(resultPath);
					env.execute();
					return "1,(2.0,0)\n" +
							"2,(4.0,1)\n" +
							"3,(6.0,2)\n" +
							"4,(8.0,3)\n" +
							"5,(10.0,4)\n";
				}
				case 4: {
				/*
				 * Test validate():
				 */
					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
					DataSet<Vertex<Long, Long>> vertices = TestGraphUtils.getLongLongVertexData(env);
					DataSet<Edge<Long, Long>> edges = TestGraphUtils.getLongLongEdgeData(env);
					Graph<Long, Long, Long> graph = new Graph<Long, Long, Long>(vertices, edges, env);
					DataSet<Boolean> result = graph.validate(new InvalidVertexIdsValidator<Long, Long, Long>());
					result.writeAsText(resultPath);
					env.execute();

					return "true\n";
				}
				case 5: {
				/*
				 * Test validate() - invalid vertex ids
				 */
					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
					DataSet<Vertex<Long, Long>> vertices = TestGraphUtils.getLongLongInvalidVertexData(env);
					DataSet<Edge<Long, Long>> edges = TestGraphUtils.getLongLongEdgeData(env);

					Graph<Long, Long, Long> graph = new Graph<Long, Long, Long>(vertices, edges, env);
					DataSet<Boolean> result = graph.validate(new InvalidVertexIdsValidator<Long, Long, Long>());
					result.writeAsText(resultPath);
					env.execute();

					return "false\n";
				}
					default:
						throw new IllegalArgumentException("Invalid program id");
				}
			}
		}

	}
