package flink.graphs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

@RunWith(Parameterized.class)
public class TestJoinWithEdges extends JavaProgramTestBase {

    private static int NUM_PROGRAMS = 15;

    private int curProgId = config.getInteger("ProgramId", -1);
    private String resultPath;
    private String expectedResult;

    public TestJoinWithEdges(Configuration config) {
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
				 * Test joinWithEdges with the input DataSet parameter identical
				 * to the edge DataSet
				 */
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                            TestGraphUtils.getLongLongEdgeData(env), env);

                    Graph<Long, Long, Long> result = graph.joinWithEdges(graph.getEdges()
                                    .map(new MapFunction<Edge<Long, Long>, Tuple3<Long, Long, Long>>() {
                                        @Override
                                        public Tuple3<Long, Long, Long> map(Edge<Long, Long> edge) throws Exception {
                                            return new Tuple3<Long, Long, Long>(edge.getSource(),
                                                    edge.getTarget(), edge.getValue());
                                        }
                                    }),
                            new MapFunction<Tuple2<Long, Long>, Long>() {

                                @Override
                                public Long map(Tuple2<Long, Long> tuple) throws Exception {
                                    return tuple.f0 + tuple.f1;
                                }
                            });

                    result.getEdges().writeAsCsv(resultPath);
                    env.execute();

                    return "1,2,24\n" +
                            "1,3,26\n" +
                            "2,3,46\n" +
                            "3,4,68\n" +
                            "3,5,70\n" +
                            "4,5,90\n" +
                            "5,1,102\n";
                }
                case 2: {
                /*
				 * Test joinWithEdges with the input DataSet passed as a parameter containing
				 * less elements than the edge DataSet, but of the same type
				 */
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                            TestGraphUtils.getLongLongEdgeData(env), env);

                    Graph<Long, Long, Long> result = graph.joinWithEdges(graph.getEdges().first(3)
                                    .map(new MapFunction<Edge<Long, Long>, Tuple3<Long, Long, Long>>() {
                                        @Override
                                        public Tuple3<Long, Long, Long> map(Edge<Long, Long> edge) throws Exception {
                                            return new Tuple3<Long, Long, Long>(edge.getSource(),
                                                    edge.getTarget(), edge.getValue());
                                        }
                                    }),
                            new MapFunction<Tuple2<Long, Long>, Long>() {

                                @Override
                                public Long map(Tuple2<Long, Long> tuple) throws Exception {
                                    return tuple.f0 + tuple.f1;
                                }
                            });

                    result.getEdges().writeAsCsv(resultPath);
                    env.execute();

                    return "1,2,24\n" +
                            "1,3,26\n" +
                            "2,3,46\n" +
                            "3,4,34\n" +
                            "3,5,35\n" +
                            "4,5,45\n" +
                            "5,1,51\n";
                }
                case 3: {
                /*
				 * Test joinWithEdges with the input DataSet passed as a parameter containing
				 * less elements than the edge DataSet and of a different type(Boolean)
				 */
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                            TestGraphUtils.getLongLongEdgeData(env), env);

                    Graph<Long, Long, Long> result = graph.joinWithEdges(graph.getEdges().first(3)
                                    .map(new MapFunction<Edge<Long, Long>, Tuple3<Long, Long, Boolean>>() {
                                        @Override
                                        public Tuple3<Long, Long, Boolean> map(Edge<Long, Long> edge) throws Exception {
                                            return new Tuple3<Long, Long, Boolean>(edge.getSource(),
                                                    edge.getTarget(), true);
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

                    result.getEdges().writeAsCsv(resultPath);
                    env.execute();

                    return "1,2,24\n" +
                            "1,3,26\n" +
                            "2,3,46\n" +
                            "3,4,34\n" +
                            "3,5,35\n" +
                            "4,5,45\n" +
                            "5,1,51\n";
                }
                case 4: {
                /*
				 * Test joinWithEdges with the input DataSet containing different keys than the edge DataSet
				 * - the iterator becomes empty.
				 */
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                            TestGraphUtils.getLongLongEdgeData(env), env);

                    Graph<Long, Long, Long> result = graph.joinWithEdges(TestGraphUtils.getLongLongLongTuple3Data(env),
                            new MapFunction<Tuple2<Long, Long>, Long>() {

                                @Override
                                public Long map(Tuple2<Long, Long> tuple) throws Exception {
                                    return tuple.f1 * 2;
                                }
                            });

                    result.getEdges().writeAsCsv(resultPath);
                    env.execute();

                    return "1,2,24\n" +
                            "1,3,26\n" +
                            "2,3,46\n" +
                            "3,4,68\n" +
                            "3,5,35\n" +
                            "4,5,45\n" +
                            "5,1,51\n";
                }
                case 5: {
                /*
    		     * Test joinWithEdges with a DataSet containing custom parametrised type input values
    			 */
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                            TestGraphUtils.getLongLongEdgeData(env), env);

                    Graph<Long, Long, Long> result = graph.joinWithEdges(TestGraphUtils.getLongLongCustomTuple3Data(env),
                            new MapFunction<Tuple2<Long, TestGraphUtils.DummyCustomParameterizedType<Float>>, Long>() {
                                public Long map(Tuple2<Long, TestGraphUtils.DummyCustomParameterizedType<Float>> tuple) throws Exception {
                                    return (long) tuple.f1.getIntField();
                                }
                            });

                    result.getEdges().writeAsCsv(resultPath);
                    env.execute();

                    return "1,2,10\n" +
                            "1,3,20\n" +
                            "2,3,30\n" +
                            "3,4,40\n" +
                            "3,5,35\n" +
                            "4,5,45\n" +
                            "5,1,51\n";
                }
                case 6: {
                /*
				 * Test joinWithEdgesOnSource with the input DataSet parameter identical
				 * to the edge DataSet
				 */
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                            TestGraphUtils.getLongLongEdgeData(env), env);

                    Graph<Long, Long, Long> result = graph.joinWithEdgesOnSource(graph.getEdges()
                                    .map(new MapFunction<Edge<Long, Long>, Tuple2<Long, Long>>() {
                                        @Override
                                        public Tuple2<Long, Long> map(Edge<Long, Long> edge) throws Exception {
                                            return new Tuple2<Long, Long>(edge.getSource(), edge.getValue());
                                        }
                                    }),
                            new MapFunction<Tuple2<Long, Long>, Long>() {

                                @Override
                                public Long map(Tuple2<Long, Long> tuple) throws Exception {
                                    return tuple.f0 + tuple.f1;
                                }
                            });

                    result.getEdges().writeAsCsv(resultPath);
                    env.execute();

                    return "1,2,24\n" +
                            "1,3,25\n" +
                            "2,3,46\n" +
                            "3,4,68\n" +
                            "3,5,69\n" +
                            "4,5,90\n" +
                            "5,1,102\n";
                }
                case 7: {
                /*
				 * Test joinWithEdgesOnSource with the input DataSet passed as a parameter containing
				 * less elements than the edge DataSet, but of the same type
				 */
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                            TestGraphUtils.getLongLongEdgeData(env), env);

                    Graph<Long, Long, Long> result = graph.joinWithEdgesOnSource(graph.getEdges().first(3)
                                    .map(new MapFunction<Edge<Long, Long>, Tuple2<Long, Long>>() {
                                        @Override
                                        public Tuple2<Long, Long> map(Edge<Long, Long> edge) throws Exception {
                                            return new Tuple2<Long, Long>(edge.getSource(), edge.getValue());
                                        }
                                    }),
                            new MapFunction<Tuple2<Long, Long>, Long>() {

                                @Override
                                public Long map(Tuple2<Long, Long> tuple) throws Exception {
                                    return tuple.f0 + tuple.f1;
                                }
                            });

                    result.getEdges().writeAsCsv(resultPath);
                    env.execute();

                    return "1,2,24\n" +
                            "1,3,25\n" +
                            "2,3,46\n" +
                            "3,4,34\n" +
                            "3,5,35\n" +
                            "4,5,45\n" +
                            "5,1,51\n";
                }
                case 8: {
                /*
				 * Test joinWithEdgesOnSource with the input DataSet passed as a parameter containing
				 * less elements than the edge DataSet and of a different type(Boolean)
				 */
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                            TestGraphUtils.getLongLongEdgeData(env), env);

                    Graph<Long, Long, Long> result = graph.joinWithEdgesOnSource(graph.getEdges().first(3)
                                    .map(new MapFunction<Edge<Long, Long>, Tuple2<Long, Boolean>>() {
                                        @Override
                                        public Tuple2<Long, Boolean> map(Edge<Long, Long> edge) throws Exception {
                                            return new Tuple2<Long, Boolean>(edge.getSource(), true);
                                        }
                                    }),
                            new MapFunction<Tuple2<Long, Boolean>, Long>() {

                                @Override
                                public Long map(Tuple2<Long, Boolean> tuple) throws Exception {
                                    if (tuple.f1) {
                                        return tuple.f0 * 2;
                                    } else {
                                        return tuple.f0;
                                    }
                                }
                            });

                    result.getEdges().writeAsCsv(resultPath);
                    env.execute();

                    return "1,2,24\n" +
                            "1,3,26\n" +
                            "2,3,46\n" +
                            "3,4,34\n" +
                            "3,5,35\n" +
                            "4,5,45\n" +
                            "5,1,51\n";
                }
                case 9: {
                /*
				 * Test joinWithEdgesOnSource with the input DataSet containing different keys than the edge DataSet
				 * - the iterator becomes empty.
				 */
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                            TestGraphUtils.getLongLongEdgeData(env), env);

                    Graph<Long, Long, Long> result = graph.joinWithEdgesOnSource(TestGraphUtils.getLongLongTuple2SourceData(env),
                            new MapFunction<Tuple2<Long, Long>, Long>() {

                                @Override
                                public Long map(Tuple2<Long, Long> tuple) throws Exception {
                                    return tuple.f1 * 2;
                                }
                            });

                    result.getEdges().writeAsCsv(resultPath);
                    env.execute();

                    return "1,2,20\n" +
                            "1,3,20\n" +
                            "2,3,60\n" +
                            "3,4,80\n" +
                            "3,5,80\n" +
                            "4,5,120\n" +
                            "5,1,51\n";
                }
                case 10: {
                /*
    		     * Test joinWithEdgesOnSource with a DataSet containing custom parametrised type input values
    			 */
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                            TestGraphUtils.getLongLongEdgeData(env), env);

                    Graph<Long, Long, Long> result = graph.joinWithEdgesOnSource(TestGraphUtils.getLongCustomTuple2SourceData(env),
                            new MapFunction<Tuple2<Long, TestGraphUtils.DummyCustomParameterizedType<Float>>, Long>() {
                                public Long map(Tuple2<Long, TestGraphUtils.DummyCustomParameterizedType<Float>> tuple) throws Exception {
                                    return (long) tuple.f1.getIntField();
                                }
                            });

                    result.getEdges().writeAsCsv(resultPath);
                    env.execute();

                    return "1,2,10\n" +
                            "1,3,10\n" +
                            "2,3,30\n" +
                            "3,4,40\n" +
                            "3,5,40\n" +
                            "4,5,45\n" +
                            "5,1,51\n";
                }
                case 11: {
                /*
				 * Test joinWithEdgesOnTarget with the input DataSet parameter identical
				 * to the edge DataSet
				 */
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                            TestGraphUtils.getLongLongEdgeData(env), env);

                    Graph<Long, Long, Long> result = graph.joinWithEdgesOnTarget(graph.getEdges()
                                    .map(new MapFunction<Edge<Long, Long>, Tuple2<Long, Long>>() {
                                        @Override
                                        public Tuple2<Long, Long> map(Edge<Long, Long> edge) throws Exception {
                                            return new Tuple2<Long, Long>(edge.getTarget(), edge.getValue());
                                        }
                                    }),
                            new MapFunction<Tuple2<Long, Long>, Long>() {

                                @Override
                                public Long map(Tuple2<Long, Long> tuple) throws Exception {
                                    return tuple.f0 + tuple.f1;
                                }
                            });

                    result.getEdges().writeAsCsv(resultPath);
                    env.execute();

                    return "1,2,24\n" +
                            "1,3,26\n" +
                            "2,3,36\n" +
                            "3,4,68\n" +
                            "3,5,70\n" +
                            "4,5,80\n" +
                            "5,1,102\n";
                }
                case 12: {
                /*
				 * Test joinWithEdgesOnTarget with the input DataSet passed as a parameter containing
				 * less elements than the edge DataSet, but of the same type
				 */
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                            TestGraphUtils.getLongLongEdgeData(env), env);

                    Graph<Long, Long, Long> result = graph.joinWithEdgesOnTarget(graph.getEdges().first(3)
                                    .map(new MapFunction<Edge<Long, Long>, Tuple2<Long, Long>>() {
                                        @Override
                                        public Tuple2<Long, Long> map(Edge<Long, Long> edge) throws Exception {
                                            return new Tuple2<Long, Long>(edge.getTarget(), edge.getValue());
                                        }
                                    }),
                            new MapFunction<Tuple2<Long, Long>, Long>() {

                                @Override
                                public Long map(Tuple2<Long, Long> tuple) throws Exception {
                                    return tuple.f0 + tuple.f1;
                                }
                            });

                    result.getEdges().writeAsCsv(resultPath);
                    env.execute();

                    result.getEdges().writeAsCsv(resultPath);
                    env.execute();

                    return "1,2,24\n" +
                            "1,3,26\n" +
                            "2,3,36\n" +
                            "3,4,34\n" +
                            "3,5,35\n" +
                            "4,5,45\n" +
                            "5,1,51\n";
                }
                case 13: {
                /*
				 * Test joinWithEdgesOnTarget with the input DataSet passed as a parameter containing
				 * less elements than the edge DataSet and of a different type(Boolean)
				 */
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                            TestGraphUtils.getLongLongEdgeData(env), env);

                    Graph<Long, Long, Long> result = graph.joinWithEdgesOnTarget(graph.getEdges().first(3)
                                    .map(new MapFunction<Edge<Long, Long>, Tuple2<Long, Boolean>>() {
                                        @Override
                                        public Tuple2<Long, Boolean> map(Edge<Long, Long> edge) throws Exception {
                                            return new Tuple2<Long, Boolean>(edge.getTarget(), true);
                                        }
                                    }),
                            new MapFunction<Tuple2<Long, Boolean>, Long>() {

                                @Override
                                public Long map(Tuple2<Long, Boolean> tuple) throws Exception {
                                    if (tuple.f1) {
                                        return tuple.f0 * 2;
                                    } else {
                                        return tuple.f0;
                                    }
                                }
                            });

                    result.getEdges().writeAsCsv(resultPath);
                    env.execute();

                    return "1,2,24\n" +
                            "1,3,26\n" +
                            "2,3,46\n" +
                            "3,4,34\n" +
                            "3,5,35\n" +
                            "4,5,45\n" +
                            "5,1,51\n";
                }
                case 14: {
                /*
				 * Test joinWithEdgesOnTarget with the input DataSet containing different keys than the edge DataSet
				 * - the iterator becomes empty.
				 */
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                            TestGraphUtils.getLongLongEdgeData(env), env);

                    Graph<Long, Long, Long> result = graph.joinWithEdgesOnTarget(TestGraphUtils.getLongLongTuple2TargetData(env),
                            new MapFunction<Tuple2<Long, Long>, Long>() {

                                @Override
                                public Long map(Tuple2<Long, Long> tuple) throws Exception {
                                    return tuple.f1 * 2;
                                }
                            });

                    result.getEdges().writeAsCsv(resultPath);
                    env.execute();

                    return "1,2,20\n" +
                            "1,3,40\n" +
                            "2,3,40\n" +
                            "3,4,80\n" +
                            "3,5,35\n" +
                            "4,5,45\n" +
                            "5,1,140\n";
                }
                case 15: {
                /*
    		     * Test joinWithEdgesOnTarget with a DataSet containing custom parametrised type input values
    			 */
                    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

                    Graph<Long, Long, Long> graph = Graph.create(TestGraphUtils.getLongLongVertexData(env),
                            TestGraphUtils.getLongLongEdgeData(env), env);

                    Graph<Long, Long, Long> result = graph.joinWithEdgesOnTarget(TestGraphUtils.getLongCustomTuple2TargetData(env),
                            new MapFunction<Tuple2<Long, TestGraphUtils.DummyCustomParameterizedType<Float>>, Long>() {
                                public Long map(Tuple2<Long, TestGraphUtils.DummyCustomParameterizedType<Float>> tuple) throws Exception {
                                    return (long) tuple.f1.getIntField();
                                }
                            });

                    result.getEdges().writeAsCsv(resultPath);
                    env.execute();

                    return "1,2,10\n" +
                            "1,3,20\n" +
                            "2,3,20\n" +
                            "3,4,40\n" +
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

