package flink.graphs;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;


public class GraphTest {

    // Assume existing graph object
    // Vertex values: 0,1,2,3
    // Edges: 0->1, 1->3, 0->3, 1->2

    Graph<Integer, Integer, Integer> graph =  new Graph<>();

    @Test
    public void testGraphCreation() {

    }

    @Test
    public void testGetVertices() throws Exception {

    }

    @Test
    public void testGetEdges() throws Exception {

    }

    @Test
    public void testMapVertices() throws Exception {
        DataSet<Vertex<Integer, Integer>> doubled= graph.mapVertices(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value * 2;
            }
        });

        // Compare the two Datasets as lists?

        List<Vertex<Integer, Integer>> doubledData = new ArrayList<>();
        doubled.output(new LocalCollectionOutputFormat<>(doubledData));



        DataSet<Vertex<Integer, Integer>>  doubledDataset = graph.getVertices()
                .map(new MapFunction<Vertex<Integer, Integer>, Vertex<Integer, Integer>>() {
                    @Override
                    public Vertex<Integer, Integer> map(Vertex<Integer, Integer> v) throws Exception {
                        return new Vertex<Integer, Integer>(v.getKey(), v.getValue() * 2);
                    }
                });
        List<Vertex<Integer, Integer>> originalDataDoubled = new ArrayList<>();
        doubledDataset.output(new LocalCollectionOutputFormat<>(originalDataDoubled));

        assertEquals(doubledData, originalDataDoubled);

        // TODO(thvasilo): Test for function that changes the type of the value
    }

    @Test
    public void testSubgraph() throws Exception {

    }
}