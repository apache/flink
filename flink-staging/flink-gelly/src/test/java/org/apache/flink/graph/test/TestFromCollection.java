package flink.graphs;

import org.apache.flink.api.common.functions.MapFunction;
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
public class TestFromCollection extends JavaProgramTestBase {
    private static int NUM_PROGRAMS = 3;

    private int curProgId = config.getInteger("ProgramId", -1);
    private String resultPath;
    private String expectedResult;

    public TestFromCollection(Configuration config) {
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

        @SuppressWarnings("serial")
        public static String runProgram(int progId, String resultPath) throws Exception {

            switch (progId) {
                case 1: {
					/*
					 * Test fromCollection(vertices, edges):
					 */
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
                    Graph<Long, Long, Long> graph = Graph.fromCollection(env, TestGraphUtils.getLongLongVertices(env),
                            TestGraphUtils.getLongLongEdges(env));

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
                case 2: {
                    /*
                     * Test fromCollection(edges) with no initial value for the vertices
                     */
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
                    Graph<Long, NullValue, Long> graph = Graph.fromCollection(env,
                            TestGraphUtils.getLongLongEdges(env));

                    graph.getVertices().writeAsCsv(resultPath);
                    env.execute();
                    return "1,(null)\n" +
                            "2,(null)\n" +
                            "3,(null)\n" +
                            "4,(null)\n" +
                            "5,(null)\n";
                }
                case 3: {
                    /*
                     * Test fromCollection(edges) with vertices initialised by a
                     * function that takes the id and doubles it
                     */
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
                    Graph<Long, Long, Long> graph = Graph.fromCollection(env, TestGraphUtils.getLongLongEdges(env),
                            new MapFunction<Long, Long>() {
                                public Long map(Long vertexId) {
                                    return vertexId * 2;
                                }
                            });

                    graph.getVertices().writeAsCsv(resultPath);
                    env.execute();
                    return "1,2\n" +
                            "2,4\n" +
                            "3,6\n" +
                            "4,8\n" +
                            "5,10\n";
                }
                default:
                    throw new IllegalArgumentException("Invalid program id");
            }
        }
    }

}