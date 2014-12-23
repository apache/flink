package flink.graphs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import flink.graphs.TestGraphUtils.DummyCustomParameterizedType;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

@RunWith(Parameterized.class)
public class TestJoinWithVertices extends JavaProgramTestBase {

    private static int NUM_PROGRAMS = 5;

    private int curProgId = config.getInteger("ProgramId", -1);
    private String resultPath;
    private String expectedResult;

    public TestJoinWithVertices(Configuration config) {
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
    public static Collection<Object[]> getConfigurations() throws IOException {

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
				 * Test joinWithVertices with the input DataSet parameter identical
				 * to the vertex DataSet
				 */
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                            TestGraphUtils.getLongLongEdgeData(env), env);

                    Graph<Long, Long, Long> result = graph.joinWithVertices(graph.getVertices()
                                    .map(new MapFunction<Vertex<Long, Long>, Tuple2<Long, Long>>() {
                                        @Override
                                        public Tuple2<Long, Long> map(Vertex<Long, Long> vertex) throws Exception {
                                            return new Tuple2<Long, Long>(vertex.getId(), vertex.getValue());
                                        }
                                    }),
                            new MapFunction<Tuple2<Long, Long>, Long>() {

                                @Override
                                public Long map(Tuple2<Long, Long> tuple) throws Exception {
                                    return tuple.f0 + tuple.f1;
                                }
                            });

                    result.getVertices().writeAsCsv(resultPath);
                    env.execute();

                    return "1,2\n" +
                            "2,4\n" +
                            "3,6\n" +
                            "4,8\n" +
                            "5,10\n";
                }
                case 2: {
				/*
				 * Test joinWithVertices with the input DataSet passed as a parameter containing
				 * less elements than the vertex DataSet, but of the same type
				 */
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                            TestGraphUtils.getLongLongEdgeData(env), env);

                    Graph<Long, Long, Long> result = graph.joinWithVertices(graph.getVertices().first(3)
                                    .map(new MapFunction<Vertex<Long, Long>, Tuple2<Long, Long>>() {
                                        @Override
                                        public Tuple2<Long, Long> map(Vertex<Long, Long> vertex) throws Exception {
                                            return new Tuple2<Long, Long>(vertex.getId(), vertex.getValue());
                                        }
                                    }),
                            new MapFunction<Tuple2<Long, Long>, Long>() {

                                @Override
                                public Long map(Tuple2<Long, Long> tuple) throws Exception {
                                    return tuple.f0 + tuple.f1;
                                }
                            });

                    result.getVertices().writeAsCsv(resultPath);
                    env.execute();

                    return "1,2\n" +
                            "2,4\n" +
                            "3,6\n" +
                            "4,4\n" +
                            "5,5\n";
                }
                case 3: {
				/*
				 * Test joinWithVertices with the input DataSet passed as a parameter containing
				 * less elements than the vertex DataSet and of a different type(Boolean)
				 */
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                            TestGraphUtils.getLongLongEdgeData(env), env);

                    Graph<Long, Long, Long> result = graph.joinWithVertices(graph.getVertices().first(3)
                                    .map(new MapFunction<Vertex<Long, Long>, Tuple2<Long, Boolean>>() {
                                        @Override
                                        public Tuple2<Long, Boolean> map(Vertex<Long, Long> vertex) throws Exception {
                                            return new Tuple2<Long, Boolean>(vertex.getId(), true);
                                        }
                                    }),
                            new MapFunction<Tuple2<Long, Boolean>, Long>() {

                                @Override
                                public Long map(Tuple2<Long, Boolean> tuple) throws Exception {
                                    if(tuple.f1) {
                                        return tuple.f0 * 2;
                                    }
                                    else {
                                        return tuple.f0;
                                    }
                                }
                            });

                    result.getVertices().writeAsCsv(resultPath);
                    env.execute();

                    return "1,2\n" +
                            "2,4\n" +
                            "3,6\n" +
                            "4,4\n" +
                            "5,5\n";
                }
                case 4: {
    				/*
    				 * Test joinWithVertices with an input DataSet containing different keys than the vertex DataSet
    				 * - the iterator becomes empty.
    				 */
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                            TestGraphUtils.getLongLongEdgeData(env), env);

                    Graph<Long, Long, Long> result = graph.joinWithVertices(TestGraphUtils.getLongLongTuple2Data(env),
                            new MapFunction<Tuple2<Long, Long>, Long>() {
                                public Long map(Tuple2<Long, Long> tuple) throws Exception {
                                    return tuple.f1;
                                }
                            });

                    result.getVertices().writeAsCsv(resultPath);
                    env.execute();

                    return "1,10\n" +
                            "2,20\n" +
                            "3,30\n" +
                            "4,40\n" +
                            "5,5\n";
                }
                case 5: {
    				/*
    				 * Test joinWithVertices with a DataSet containing custom parametrised type input values
    				 */
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                            TestGraphUtils.getLongLongEdgeData(env), env);

                    Graph<Long, Long, Long> result = graph.joinWithVertices(TestGraphUtils.getLongCustomTuple2Data(env),
                            new MapFunction<Tuple2<Long, DummyCustomParameterizedType<Float>>, Long>() {
                                public Long map(Tuple2<Long, DummyCustomParameterizedType<Float>> tuple) throws Exception {
                                    return (long) tuple.f1.getIntField();
                                }
                            });

                    result.getVertices().writeAsCsv(resultPath);
                    env.execute();

                    return "1,10\n" +
                            "2,20\n" +
                            "3,30\n" +
                            "4,40\n" +
                            "5,5\n";
                }
                default:
                    throw new IllegalArgumentException("Invalid program id");
            }
        }
    }
}
