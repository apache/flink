package flink.graphs;


import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.util.Collector;
import org.junit.Before;
import org.junit.Test;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class GraphTest implements Serializable{

    // Assume existing graph object
    // Tuple2 ids and values: 0,1,2,3
    // Edges: 0->1, 1->3, 0->3, 1->2

    static Graph<Integer, Integer, Integer> graph;
    static ExecutionEnvironment env;

    @Before
    public void testSetUp() {
        env = ExecutionEnvironment.getExecutionEnvironment();
        setUpGraph();
    }


    public static void setUpGraph() {

        List<Tuple2<Integer, Integer>> Tuple2List = new ArrayList<Tuple2<Integer, Integer>>();

        for (int i = 0; i < 4; i++) {
            Tuple2<Integer, Integer> v = new Tuple2<Integer, Integer>(i, i);
            Tuple2List.add(v);
        }


        List<Tuple3<Integer, Integer, Integer>> edgeList = new ArrayList<>();

        edgeList.add(new Tuple3<Integer, Integer, Integer>(0, 1, 0));
        edgeList.add(new Tuple3<Integer, Integer, Integer>(1, 3, 0));
        edgeList.add(new Tuple3<Integer, Integer, Integer>(0, 3, 0));
        edgeList.add(new Tuple3<Integer, Integer, Integer>(1, 2, 0));

        DataSet<Tuple2<Integer, Integer>> vertices = env.fromCollection(Tuple2List);
        DataSet<Tuple3<Integer, Integer, Integer>> edges = env.fromCollection(edgeList);

        graph = new Graph<Integer, Integer, Integer>(vertices, edges);
    }

    @Test
    public void testCreate() throws Exception {

        List<Tuple2<Integer, Integer>> Tuple2List = new ArrayList<Tuple2<Integer, Integer>>();

        for (int i = 0; i < 4; i++) {
            Tuple2<Integer, Integer> v = new Tuple2<Integer, Integer>(i, i);
            Tuple2List.add(v);
        }


        List<Tuple3<Integer, Integer, Integer>> edgeList = new ArrayList<>();

        edgeList.add(new Tuple3<Integer, Integer, Integer>(0, 1, 0));
        edgeList.add(new Tuple3<Integer, Integer, Integer>(1, 3, 0));
        edgeList.add(new Tuple3<Integer, Integer, Integer>(0, 3, 0));
        edgeList.add(new Tuple3<Integer, Integer, Integer>(1, 2, 0));

        DataSet<Tuple2<Integer, Integer>> vertices = env.fromCollection(Tuple2List);
        DataSet<Tuple3<Integer, Integer, Integer>> edges = env.fromCollection(edgeList);

        Graph<Integer, Integer, Integer> g = Graph.create(vertices, edges);

        g.getVertices().print();

        env.execute();
    }

    @Test
    public void testGetVertices() throws Exception {
        throw new NotImplementedException();
    }

    @Test
    public void testGetEdges() throws Exception {
        throw new NotImplementedException();
    }

    @Test
    public void testMapVertices() throws Exception {

        DataSet<Tuple2<Integer, Integer>> doubled= graph.mapVertices(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value * 2;
            }
        });

        // Compare the two Datasets as lists?

        List<Tuple2<Integer, Integer>> doubledData = new ArrayList<>();
        doubled.output(new LocalCollectionOutputFormat<>(doubledData));



        DataSet<Tuple2<Integer, Integer>>  doubledDataset = graph.getVertices()
                .map(new MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> v) throws Exception {
                        return new Tuple2<Integer, Integer>(v.f0, v.f1 * 2);
                    }
                });
        List<Tuple2<Integer, Integer>> originalDataDoubled = new ArrayList<>();
        doubledDataset.output(new LocalCollectionOutputFormat<>(originalDataDoubled));

        assertEquals(doubledData, originalDataDoubled);

        // TODO(thvasilo): Test for function that changes the type of the value

        doubled.print();
        graph.getVertices().print();

        env.execute();

    }

    @Test
    public void testSubgraph() throws Exception {
        throw new NotImplementedException();
    }

    @Test
    public void testPga() throws Exception {
        // Test pga by running connected components
        // Expected output is that all vertices end up with the same attribute, 0

        // Send the vertex attribute to all neighbors
        CoGroupFunction<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Integer>, Tuple2<Integer, Integer>>
                sendAttribute =
                new CoGroupFunction<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Integer>, Tuple2<Integer, Integer>>() {
            @Override
            public void coGroup(Iterable<Tuple2<Integer, Integer>> vertices,
                                Iterable<Tuple3<Integer, Integer, Integer>> edges,
                                Collector<Tuple2<Integer, Integer>> tuple2Collector) throws Exception {
                for (Tuple2<Integer, Integer> vertex : vertices) {
                    for (Tuple3<Integer, Integer, Integer> edge: edges) {
                        tuple2Collector.collect(new Tuple2<Integer, Integer>(edge.f1, vertex.f1));
                    }
                }
            }
        };

        // Gather all messages and keep the message with the smallest attribute
        GroupReduceFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>
                gatherAttributes =
                new GroupReduceFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    @Override
                    public void reduce(Iterable<Tuple2<Integer, Integer>> messages,
                                       Collector<Tuple2<Integer, Integer>> msgCollector) throws Exception {

                        Tuple2<Integer, Integer> minTuple = new Tuple2<Integer, Integer>(Integer.MAX_VALUE, Integer.MAX_VALUE);
                        for (Tuple2<Integer, Integer> message : messages) {
                            if (message.f1 < minTuple.f1) {
                                minTuple = message.copy();
                            }
                        }
                        msgCollector.collect(minTuple);
                    }
                };

        // Check if the produced message is smaller than the current vertex attribute, if yes change attribute
        FlatJoinFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>
                apply =
                new FlatJoinFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    @Override
                    public void join(Tuple2<Integer, Integer> msg,
                                     Tuple2<Integer, Integer> vertex,
                                     Collector<Tuple2<Integer, Integer>> vertexCollector) throws Exception {
                        if (msg.f1 < vertex.f1) {
                            vertexCollector.collect(msg.copy());
                        }
                    }
                };


        // Run the pga iterations
        Graph<Integer, Integer, Integer> connected = graph.pga(sendAttribute, gatherAttributes, apply, 100);

        DataSet<Tuple2<Integer, Integer>> conVerts = connected.getVertices();

        // All vertices should end up with attribute 0
        conVerts.print();
        //TODO(thvasilo): Automate correctness testing

        env.execute();

    }

}