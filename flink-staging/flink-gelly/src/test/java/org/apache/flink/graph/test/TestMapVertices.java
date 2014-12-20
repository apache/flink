package flink.graphs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import flink.graphs.TestGraphUtils.DummyCustomParameterizedType;
import flink.graphs.TestGraphUtils.DummyCustomType;

@RunWith(Parameterized.class)
public class TestMapVertices extends JavaProgramTestBase {

	private static int NUM_PROGRAMS = 5;
	
	private int curProgId = config.getInteger("ProgramId", -1);
	private String resultPath;
	private String expectedResult;
	
	public TestMapVertices(Configuration config) {
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
				 * Test mapVertices() keeping the same value type
				 */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
						TestGraphUtils.getLongLongEdgeData(env));
				
				DataSet<Vertex<Long, Long>> mappedVertices = graph.mapVertices(new MapFunction<Vertex<Long, Long>, Long>() {
					public Long map(Vertex<Long, Long> value) throws Exception {
						return value.getValue()+1;
					}
				}).getVertices();
				
				mappedVertices.writeAsCsv(resultPath);
				env.execute();
				return "1,2\n" +
				"2,3\n" +
				"3,4\n" +
				"4,5\n" +
				"5,6\n";
			}
			case 2: {
				/*
				 * Test mapVertices() and change the value type to String
				 */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
						TestGraphUtils.getLongLongEdgeData(env));
				
				DataSet<Vertex<Long, String>> mappedVertices = graph.mapVertices(new MapFunction<Vertex<Long, Long>, String>() {
					public String map(Vertex<Long, Long> vertex) throws Exception {
						String stringValue;
						if (vertex.getValue() == 1) {
							stringValue = "one";
						}
						else if (vertex.getValue() == 2) {
							stringValue = "two";
						}
						else if (vertex.getValue() == 3) {
							stringValue = "three";
						}
						else if (vertex.getValue() == 4) {
							stringValue = "four";
						}
						else if (vertex.getValue() == 5) {
							stringValue = "five";
						}
						else {
							stringValue = "";
						}
						return stringValue;
					}
				}).getVertices();
				
				mappedVertices.writeAsCsv(resultPath);
				env.execute();
				return "1,one\n" +
				"2,two\n" +
				"3,three\n" +
				"4,four\n" +
				"5,five\n";
			}
			case 3: {
				/*
				 * Test mapVertices() and change the value type to a Tuple1
				 */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
						TestGraphUtils.getLongLongEdgeData(env));
				
				DataSet<Vertex<Long, Tuple1<Long>>> mappedVertices = graph.mapVertices(new MapFunction<Vertex<Long, Long>, Tuple1<Long>>() {
					public Tuple1<Long> map(Vertex<Long, Long> vertex) throws Exception {
						Tuple1<Long> tupleValue = new Tuple1<Long>();
						tupleValue.setFields(vertex.getValue());
						return tupleValue;
					}
				}).getVertices();
				
				mappedVertices.writeAsCsv(resultPath);
				env.execute();
				return "1,(1)\n" +
				"2,(2)\n" +
				"3,(3)\n" +
				"4,(4)\n" +
				"5,(5)\n";
			}
			case 4: {
				/*
				 * Test mapVertices() and change the value type to a custom type
				 */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
						TestGraphUtils.getLongLongEdgeData(env));
				
				DataSet<Vertex<Long, DummyCustomType>> mappedVertices = graph.mapVertices(new MapFunction<Vertex<Long, Long>, DummyCustomType>() {
					public DummyCustomType map(Vertex<Long, Long> vertex) throws Exception {
						DummyCustomType dummyValue = new DummyCustomType();
						dummyValue.setIntField(vertex.getValue().intValue());						
						return dummyValue;
					}
				}).getVertices();
				
				mappedVertices.writeAsCsv(resultPath);
				env.execute();
				return "1,(T,1)\n" +
				"2,(T,2)\n" +
				"3,(T,3)\n" +
				"4,(T,4)\n" +
				"5,(T,5)\n";
			}
			case 5: {
				/*
				 * Test mapVertices() and change the value type to a parameterized custom type
				 */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
						TestGraphUtils.getLongLongEdgeData(env));
				
				DataSet<Vertex<Long, DummyCustomParameterizedType<Double>>> mappedVertices = graph.mapVertices(
						new MapFunction<Vertex<Long, Long>, DummyCustomParameterizedType<Double>>() {
					public DummyCustomParameterizedType<Double> map(Vertex<Long, Long> vertex) throws Exception {
						DummyCustomParameterizedType<Double> dummyValue = new DummyCustomParameterizedType<Double>();
						dummyValue.setIntField(vertex.getValue().intValue());
						dummyValue.setTField(new Double(vertex.getValue()));						
						return dummyValue;
					}
				}).getVertices();
				
				mappedVertices.writeAsCsv(resultPath);
				env.execute();
				return "1,(1.0,1)\n" +
				"2,(2.0,2)\n" +
				"3,(3.0,3)\n" +
				"4,(4.0,4)\n" +
				"5,(5.0,5)\n";
			}
			default: 
				throw new IllegalArgumentException("Invalid program id");
			}
		}
	}

}
