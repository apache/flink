package flink.graphs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.types.NullValue;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import flink.graphs.TestGraphUtils.DummyCustomParameterizedType;
import flink.graphs.TestGraphUtils.DummyCustomType;

@RunWith(Parameterized.class)
public class TestGraphCreation extends JavaProgramTestBase {

	private static int NUM_PROGRAMS = 6;
	
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
				Graph<Long, NullValue, Long> graph = Graph.create(TestGraphUtils.getLongLongEdgeData(env));

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
						});

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
				 * Test create() with edge dataset and a mapper that assigns a double constant as value
		         */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				Graph<Long, Double, Long> graph = Graph.create(TestGraphUtils.getLongLongEdgeData(env), 
						new MapFunction<Long, Double>() {
							public Double map(Long value) {
								return 0.1d;
							}
						});

				graph.getVertices().writeAsCsv(resultPath);
				env.execute();
				return "1,0.1\n" +
					"2,0.1\n" +
					"3,0.1\n" + 
					"4,0.1\n" + 
					"5,0.1\n";
			}
			case 4: {
				/*
				 * Test create() with edge dataset and a mapper that assigns a Tuple2 as value
				 */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				Graph<Long, Tuple2<Long, Long>, Long> graph = Graph.create(
						TestGraphUtils.getLongLongEdgeData(env), new MapFunction<Long, Tuple2<Long, Long>>() {
							public Tuple2<Long, Long> map(Long vertexId) {
								return new Tuple2<Long, Long>(vertexId*2, 42l);
							}
						});

				graph.getVertices().writeAsCsv(resultPath);
				env.execute();
				return "1,(2,42)\n" +
					"2,(4,42)\n" +
					"3,(6,42)\n" + 
					"4,(8,42)\n" + 
					"5,(10,42)\n";
			}
			case 5: {
				/*
				 * Test create() with edge dataset and a mapper that assigns a custom vertex value
				 */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				Graph<Long, DummyCustomType, Long> graph = Graph.create(
						TestGraphUtils.getLongLongEdgeData(env), new MapFunction<Long, DummyCustomType>() {
							public DummyCustomType map(Long vertexId) {
								return new DummyCustomType(vertexId.intValue()-1, false);
							}
						});

				graph.getVertices().writeAsCsv(resultPath);
				env.execute();
				return "1,(F,0)\n" +
					"2,(F,1)\n" +
					"3,(F,2)\n" + 
					"4,(F,3)\n" + 
					"5,(F,4)\n";
			}
			case 6: {
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
						});

				graph.getVertices().writeAsCsv(resultPath);
				env.execute();
				return "1,(2.0,0)\n" +
					"2,(4.0,1)\n" +
					"3,(6.0,2)\n" + 
					"4,(8.0,3)\n" + 
					"5,(10.0,4)\n";
			}
			default: 
				throw new IllegalArgumentException("Invalid program id");
			}
		}
	}
	
}
