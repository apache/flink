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


        DataSet<Edge<Long, Double>> edges = getlinks(env);



        Graph<Long, Double, Double> graph = Graph.fromDataSet(vertices, edges, env);

        DataSet<Tuple3<Long, Double, Double>> hubAndAuthority = new Hits_Class(graph,3).run();

        hubAndAuthority.print();

        env.execute("javid");

    }

    public static DataSet<Edge<Long, Double> > getlinks(ExecutionEnvironment en){
        return en.generateSequence(1,1).flatMap(new FlatMapFunction<Long, Edge<Long,Double>>() {
            @Override
            public void flatMap(Long value, Collector<Edge<Long,Double>> out) throws Exception {

                out.collect(new Edge<Long, Double>(1l,2l, 2.0));
                out.collect(new Edge<Long, Double>(2l,1l, 4.0));
                out.collect(new Edge<Long, Double>(2l,3l, 3.0));
                out.collect(new Edge<Long, Double>(3l,1l, 5.0));
                out.collect(new Edge<Long, Double>(3l,2l, 6.0));
            }
        });

    }

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
