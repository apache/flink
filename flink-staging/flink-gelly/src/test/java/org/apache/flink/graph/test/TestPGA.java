//package flink.graphs;
//
//import java.util.ArrayList;
//import java.util.List;
//
//import org.apache.flink.api.common.functions.CoGroupFunction;
//import org.apache.flink.api.common.functions.FlatJoinFunction;
//import org.apache.flink.api.common.functions.GroupReduceFunction;
//import org.apache.flink.api.java.DataSet;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.tuple.Tuple3;
//import org.apache.flink.util.Collector;
//import org.junit.Before;
//import org.junit.Test;
//
//public class TestPGA {
//
//    // Assume existing graph object
//    // Tuple2 ids and values: 0,1,2,3
//    // Edges: 0->1, 1->3, 0->3, 1->2
//
//    static Graph<Integer, Integer, Integer> graph;
//    static ExecutionEnvironment env;
//
//    @Before
//    public void testSetUp() {
//        env = ExecutionEnvironment.getExecutionEnvironment();
//        setUpGraph();
//    }
//
//
//    public static void setUpGraph() {
//
//        List<Tuple2<Integer, Integer>> Tuple2List = new ArrayList<Tuple2<Integer, Integer>>();
//
//        for (int i = 0; i < 4; i++) {
//            Tuple2<Integer, Integer> v = new Tuple2<Integer, Integer>(i, i);
//            Tuple2List.add(v);
//        }
//
//
//        List<Tuple3<Integer, Integer, Integer>> edgeList = new ArrayList<>();
//
//        edgeList.add(new Tuple3<Integer, Integer, Integer>(0, 1, 0));
//        edgeList.add(new Tuple3<Integer, Integer, Integer>(1, 3, 0));
//        edgeList.add(new Tuple3<Integer, Integer, Integer>(0, 3, 0));
//        edgeList.add(new Tuple3<Integer, Integer, Integer>(1, 2, 0));
//
//        DataSet<Tuple2<Integer, Integer>> vertices = env.fromCollection(Tuple2List);
//        DataSet<Tuple3<Integer, Integer, Integer>> edges = env.fromCollection(edgeList);
//
//        graph = new Graph<Integer, Integer, Integer>(vertices, edges, env);
//    }
//    @SuppressWarnings("serial")
//	@Test
//    public void testPga() throws Exception {
//        // Test pga by running connected components
//        // Expected output is that all vertices end up with the same attribute, 0
//
//        // Send the vertex attribute to all neighbors
//        CoGroupFunction<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Integer>, Tuple2<Integer, Integer>>
//                sendAttribute =
//                new CoGroupFunction<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Integer>, Tuple2<Integer, Integer>>() {
//            @Override
//            public void coGroup(Iterable<Tuple2<Integer, Integer>> vertices,
//                                Iterable<Tuple3<Integer, Integer, Integer>> edges,
//                                Collector<Tuple2<Integer, Integer>> tuple2Collector) throws Exception {
//                for (Tuple2<Integer, Integer> vertex : vertices) {
//                    for (Tuple3<Integer, Integer, Integer> edge: edges) {
//                        tuple2Collector.collect(new Tuple2<Integer, Integer>(edge.f1, vertex.f1));
//                    }
//                }
//            }
//        };
//
//        // Gather all messages and keep the message with the smallest attribute
//        GroupReduceFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>
//                gatherAttributes =
//                new GroupReduceFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
//                    @Override
//                    public void reduce(Iterable<Tuple2<Integer, Integer>> messages,
//                                       Collector<Tuple2<Integer, Integer>> msgCollector) throws Exception {
//
//                        Tuple2<Integer, Integer> minTuple = new Tuple2<Integer, Integer>(Integer.MAX_VALUE, Integer.MAX_VALUE);
//                        for (Tuple2<Integer, Integer> message : messages) {
//                            if (message.f1 < minTuple.f1) {
//                                minTuple = message.copy();
//                            }
//                        }
//                        msgCollector.collect(minTuple);
//                    }
//                };
//
//        // Check if the produced message is smaller than the current vertex attribute, if yes change attribute
//        FlatJoinFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>
//                apply =
//                new FlatJoinFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
//                    @Override
//                    public void join(Tuple2<Integer, Integer> msg,
//                                     Tuple2<Integer, Integer> vertex,
//                                     Collector<Tuple2<Integer, Integer>> vertexCollector) throws Exception {
//                        if (msg.f1 < vertex.f1) {
//                            vertexCollector.collect(msg.copy());
//                        }
//                    }
//                };
//
//
//        // Run the pga iterations
//        Graph<Integer, Integer, Integer> connected = graph.pga(sendAttribute, gatherAttributes, apply, 100);
//
//        DataSet<Tuple2<Integer, Integer>> conVerts = connected.getVertices();
//
//        // All vertices should end up with attribute 0
//        conVerts.print();
//        //TODO(thvasilo): Automate correctness testing
//
//        env.execute();
//
//    }
//}
