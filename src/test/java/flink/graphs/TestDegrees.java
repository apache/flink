package flink.graphs;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.types.NullValue;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

@RunWith(Parameterized.class)
public class TestDegrees extends JavaProgramTestBase {

    private static int NUM_PROGRAMS = 6;

    private int curProgId = config.getInteger("ProgramId", -1);
    private String resultPath;
    private String expectedResult;

    public TestDegrees(Configuration config) {
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

    @Parameterized.Parameters
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

            switch (progId) {
                case 1: {
				/*
				 * Test outDegrees()
				 */
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                            TestGraphUtils.getLongLongEdgeData(env), env);

                    graph.outDegrees().writeAsCsv(resultPath);
                    env.execute();
                    return "1,2\n" +
                            "2,1\n" +
                            "3,2\n" +
                            "4,1\n" +
                            "5,1\n";
                }
                case 2: {
				/*
				 * Test outDegrees() no outgoing edges
				 */
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                            TestGraphUtils.getLongLongEdgeDataWithZeroDegree(env), env);

                    graph.outDegrees().writeAsCsv(resultPath);
                    env.execute();
                    return "1,3\n" +
                            "2,1\n" +
                            "3,1\n" +
                            "4,1\n" +
                            "5,0\n";
                }
                case 3: {
				/*
				 * Test inDegrees()
				 */
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                            TestGraphUtils.getLongLongEdgeData(env), env);

                    graph.inDegrees().writeAsCsv(resultPath);
                    env.execute();
                    return "1,1\n" +
                            "2,1\n" +
                            "3,2\n" +
                            "4,1\n" +
                            "5,2\n";
                }
                case 4: {
				/*
				 * Test inDegrees() no ingoing edge
				 */
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                            TestGraphUtils.getLongLongEdgeDataWithZeroDegree(env), env);

                    graph.inDegrees().writeAsCsv(resultPath);
                    env.execute();
                    return "1,0\n" +
                            "2,1\n" +
                            "3,1\n" +
                            "4,1\n" +
                            "5,3\n";
                }
                case 5: {
				/*
				 * Test getDegrees()
				 */
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                            TestGraphUtils.getLongLongEdgeData(env), env);

                    graph.getDegrees().writeAsCsv(resultPath);
                    env.execute();
                    return "1,3\n" +
                            "2,2\n" +
                            "3,4\n" +
                            "4,2\n" +
                            "5,3\n";
                }
                case 6: {
                /*
				 * Test getDegrees() with disconnected data
				 */
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, NullValue, Long> graph =
                            Graph.create(TestGraphUtils.getDisconnectedLongLongEdgeData(env), env);

                    graph.outDegrees().writeAsCsv(resultPath);
                    env.execute();
                    return "1,2\n" +
                            "2,1\n" +
                            "3,0\n" +
                            "4,1\n" +
                            "5,0\n";
                }
                default:
                    throw new IllegalArgumentException("Invalid program id");

            }
        }
    }
}
