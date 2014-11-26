package flink.graphs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestForeachEdge extends JavaProgramTestBase {

	private static int NUM_PROGRAMS = 1;
	
	private int curProgId = config.getInteger("ProgramId", -1);
	private String resultPath;
	private String expectedResult;
	
	public TestForeachEdge(Configuration config) {
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
						TestGraphUtils.getLongLongEdgeData(env));
				
				DataSet<Tuple2<Long, Long>> verticesWithLowestOutNeighbor = 
						graph.foreachEdge(new OutEdgesFunction<Long, Long, Long, Tuple2<Long, Long>>() {

					public Tuple2<Long, Long> iterateOutEdges(
							Tuple2<Long, Long> v,
							Iterable<Tuple3<Long, Long, Long>> outEdges) {
						
						long weight = Long.MAX_VALUE;
						long minNeighorId = 0;
						
						for (Tuple3<Long, Long, Long> edge: outEdges) {
							if (edge.f2 < weight) {
								weight = edge.f2;
								minNeighorId = edge.f1;
							}
						}
						return new Tuple2<Long, Long>(v.f0, minNeighorId);
					}
				});
				env.execute();
				return "something";
			}
			default: 
				throw new IllegalArgumentException("Invalid program id");
			}
		}
	}
	
}
