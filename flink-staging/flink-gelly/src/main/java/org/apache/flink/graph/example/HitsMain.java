package org.apache.flink.graph.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;

/**
 * Created by Ahamad Javid Mayar and Ahmad Fahim Azizi on.
 * This program implement the HITS algorithm.
 * the result is combination of three values. first is the ID, Second is the Hub Value and third is  the Authority Value.
 */
public class HitsMain {

    public static void main(String args [] ) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Vertex<Long, Double>> vertices = getVertices(env);

        DataSet<Edge<Long, Double>> edges = getEdges(env);

        //create a graph from vertices(list of vertices created with getVertices function)
        // and edges(list of Edges created with getEdges function).
        Graph<Long, Double, Double> graph = Graph.fromDataSet(vertices, edges, env);

        // add  graph to Hits_Class with iteration value.
        DataSet<Tuple3<Long, Double, Double>> hubAndAuthority = new Hits_Class(graph,3).run();


        // print the retrieved data set.
        hubAndAuthority.print();

        env.execute("Hits Example");

    }

    /**
     * this function create a set of links (Edges).
     * @param en (environment variable)
     * @return Datset of Edges.
     */

    public static DataSet<Edge<Long, Double> > getEdges(ExecutionEnvironment en){
        return en.generateSequence(1,1).flatMap(new FlatMapFunction<Long, Edge<Long,Double>>() {
            @Override
            public void flatMap(Long value, Collector<Edge<Long,Double>> out) throws Exception {

                out.collect(new Edge<Long, Double>(1l,2l, 0.0));
                out.collect(new Edge<Long, Double>(2l,1l, 0.0));
                out.collect(new Edge<Long, Double>(2l,3l, 0.0));
                out.collect(new Edge<Long, Double>(3l,1l, 0.0));
                out.collect(new Edge<Long, Double>(3l,2l, 0.0));
            }
        });

    }

    /**
     * this function create a set of nodes (Vertices).
     * @param en (environment variable)
     * @return DataSet of Vertices.
     */
    public static DataSet<Vertex<Long, Double>> getVertices(ExecutionEnvironment en){
        return en.generateSequence(1,1).flatMap(new FlatMapFunction<Long, Vertex<Long,Double>>() {
            @Override
            public void flatMap(Long value, Collector<Vertex<Long,Double>> out) throws Exception {

                out.collect(new Vertex<Long, Double>(1l, 1.0));
                out.collect(new Vertex<Long, Double>(2l, 1.0));
                out.collect(new Vertex<Long, Double>(3l, 1.0));
            }
        });

    }
}
