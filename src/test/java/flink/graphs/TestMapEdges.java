package flink.graphs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import flink.graphs.TestGraphUtils.DummyCustomParameterizedType;
import flink.graphs.TestGraphUtils.DummyCustomType;

@RunWith(Parameterized.class)
public class TestMapEdges extends JavaProgramTestBase {

	private static int NUM_PROGRAMS = 5;
	
	private int curProgId = config.getInteger("ProgramId", -1);
	private String resultPath;
	private String expectedResult;
	
	public TestMapEdges(Configuration config) {
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
				 * Test mapEdges() keeping the same value type
				 */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
						TestGraphUtils.getLongLongEdgeData(env));
				
				DataSet<Tuple3<Long, Long, Long>> mappedEdges = graph.mapEdges(new MapFunction<Long, Long>() {
					public Long map(Long value) throws Exception {
						return value+1;
					}
				});
				
				mappedEdges.writeAsCsv(resultPath);
				env.execute();
				return "1,2,13\n" +
				"1,3,14\n" +
				"2,3,24\n" +
				"3,4,35\n" +
				"3,5,36\n" + 
				"4,5,46\n" + 
				"5,1,52\n";
			}
			case 2: {
				/*
				 * Test mapEdges() and change the value type to String
				 */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
						TestGraphUtils.getLongLongEdgeData(env));
				
				DataSet<Tuple3<Long, Long, String>> mappedEdges = graph.mapEdges(new MapFunction<Long, String>() {
					public String map(Long value) throws Exception {
						return String.format("string(%d)", value);
					}
				});
				
				mappedEdges.writeAsCsv(resultPath);
				env.execute();
				return "1,2,string(12)\n" +
				"1,3,string(13)\n" +
				"2,3,string(23)\n" +
				"3,4,string(34)\n" +
				"3,5,string(35)\n" + 
				"4,5,string(45)\n" + 
				"5,1,string(51)\n";
			}
			case 3: {
				/*
				 * Test mapEdges() and change the value type to a Tuple1
				 */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
						TestGraphUtils.getLongLongEdgeData(env));
				
				DataSet<Tuple3<Long, Long, Tuple1<Long>>> mappedEdges = graph.mapEdges(new MapFunction<Long, Tuple1<Long>>() {
					public Tuple1<Long> map(Long value) throws Exception {
						Tuple1<Long> tupleValue = new Tuple1<Long>();
						tupleValue.setFields(value);
						return tupleValue;
					}
				});
				
				mappedEdges.writeAsCsv(resultPath);
				env.execute();
				return "1,2,(12)\n" +
				"1,3,(13)\n" +
				"2,3,(23)\n" +
				"3,4,(34)\n" +
				"3,5,(35)\n" + 
				"4,5,(45)\n" + 
				"5,1,(51)\n";
			}
			case 4: {
				/*
				 * Test mapEdges() and change the value type to a custom type
				 */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
						TestGraphUtils.getLongLongEdgeData(env));
				
				DataSet<Tuple3<Long, Long, DummyCustomType>> mappedEdges = graph.mapEdges(new MapFunction<Long, DummyCustomType>() {
					public DummyCustomType map(Long value) throws Exception {
						DummyCustomType dummyValue = new DummyCustomType();
						dummyValue.setIntField(value.intValue());						
						return dummyValue;
					}
				});
				
				mappedEdges.writeAsCsv(resultPath);
				env.execute();
				return "1,2,(T,12)\n" +
				"1,3,(T,13)\n" +
				"2,3,(T,23)\n" +
				"3,4,(T,34)\n" +
				"3,5,(T,35)\n" + 
				"4,5,(T,45)\n" + 
				"5,1,(T,51)\n";
			}
			case 5: {
				/*
				 * Test mapEdges() and change the value type to a parameterized custom type
				 */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
						TestGraphUtils.getLongLongEdgeData(env));
				
				DataSet<Tuple3<Long, Long, DummyCustomParameterizedType<Double>>> mappedEdges = graph.mapEdges(
						new MapFunction<Long, DummyCustomParameterizedType<Double>>() {
					public DummyCustomParameterizedType<Double> map(Long value) throws Exception {
						DummyCustomParameterizedType<Double> dummyValue = new DummyCustomParameterizedType<Double>();
						dummyValue.setIntField(value.intValue());
						dummyValue.setTField(new Double(value));						
						return dummyValue;
					}
				});
				
				mappedEdges.writeAsCsv(resultPath);
				env.execute();
				return "1,2,(12.0,12)\n" +
				"1,3,(13.0,13)\n" +
				"2,3,(23.0,23)\n" +
				"3,4,(34.0,34)\n" +
				"3,5,(35.0,35)\n" + 
				"4,5,(45.0,45)\n" + 
				"5,1,(51.0,51)\n";
			}
			default: 
				throw new IllegalArgumentException("Invalid program id");
			}
		}
	}
	
}
