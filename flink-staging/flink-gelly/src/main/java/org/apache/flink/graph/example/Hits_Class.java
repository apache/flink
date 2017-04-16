package org.apache.flink.graph.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.*;

/**
 * Created by Ahamd Javid Mayar and  Ahamd Fahim Azizi.
 *
 */
public class Hits_Class {

    static Graph< Long, Double, Double > mainGraph;

    static int iterationValue;


    public Hits_Class( Graph< Long, Double, Double > myGraph, int iteration ){

        mainGraph = myGraph;
        iterationValue = iteration;

    }

    /**
     * This function compute the hub and authority values from graph
     * and update the graph in each iteration with hubValues and authorityValues
     *
     * return Dataset< Tuple3 > with IDs, Hub Values, Authority Values
     *
     */
    public static DataSet<Tuple3<Long, Double, Double>> run(){

        DataSet<Tuple2<Long, Double>> authorityValues = null;
        DataSet<Tuple2<Long, Double>> hubValues = null;
        Graph<Long, Double, Double> updatedGraph = mainGraph;

        for(int i = 0; i < iterationValue ; i++ ) {
            authorityValues = authorityUpdateRule(updatedGraph);


            updatedGraph = updateGraph(updatedGraph, authorityValues);

            hubValues = hubUpdateRule(updatedGraph);


            updatedGraph = updateGraph(updatedGraph, hubValues);

        }


        DataSet<Tuple3<Long, Double, Double> > HubAuthory=hubValues.join(authorityValues).where(0).equalTo(0).map(new HubAuthorityScore());
        return  HubAuthory;


    }

    /**
     * This Class have the function of map which join the hub and authority DataSets to one Tuple3 dataSet Example < ID , HubValue, AuthorityValue >
     * @param1 HubValueDataSet
     * @param2 AuthorityValueDatSet
     *
     * return Tuple3DataSet
     */

    public static class HubAuthorityScore implements MapFunction<Tuple2<Tuple2<Long,Double>,Tuple2<Long,Double>>,Tuple3<Long, Double,Double>>{

        @Override
        public Tuple3<Long, Double, Double> map(Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>> value) throws Exception {
            return new Tuple3<Long, Double, Double>(value.f0.f0,value.f0.f1,value.f1.f1);
        }
    }

    /**
     * This function create the authority DataSet from given graph
     * @param1 graph
     *
     * return DataSet<Tuple2> with Authority Values for each node.
     */

    public static DataSet<Tuple2<Long, Double>> authorityUpdateRule( Graph< Long, Double, Double> myGraph ){

        DataSet<Tuple2<Long, Double>> verticesWithSum = myGraph.reduceOnNeighbors(new SumValues(), EdgeDirection.IN);
        return verticesWithSum;

    }

    /**
     * This function create the hubValues DataSet from given graph
     * @param1 graph
     *
     * return DataSet<Tuple2> with Hub Values for each node.
     */

    public static DataSet<Tuple2<Long, Double>> hubUpdateRule( Graph< Long, Double, Double> myGraph ){

        DataSet<Tuple2<Long, Double>> verticesWithSum = myGraph.reduceOnNeighbors(new SumValues(), EdgeDirection.OUT);
        return verticesWithSum;
    }

    /**
     * This function update the graph vertices values by give DatSet<Tuple2>
     * @param1 graph
     *
     * return graph with updated vertices Values.
     */

    public static Graph<Long, Double, Double > updateGraph( Graph<Long, Double, Double> myGraph, DataSet<Tuple2< Long, Double >> myDataSet ){

        Graph<Long, Double, Double> updatedGraph = myGraph.joinWithVertices(myDataSet,new MapFunction<Tuple2<Double, Double>, Double>() {

            public Double map(Tuple2<Double, Double> value) {
                return value.f0 = value.f1;
            }
        });

        return updatedGraph;

    }

    // function to sum the neighbor values
    static final class SumValues implements ReduceNeighborsFunction<Double> {

        @Override
        public Double reduceNeighbors(Double firstNeighbor, Double secondNeighbor) {
            return firstNeighbor + secondNeighbor;
        }
    }
}
