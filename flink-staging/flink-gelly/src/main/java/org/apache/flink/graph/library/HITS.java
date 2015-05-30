package org.apache.flink.graph.library;

import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.graph.utils.Hits;


/**
 *
 * This class implements the HITS algorithm by using flink Gelly API
 *Hyperlink-Induced Topic Search (HITS; also known as hubs and authorities) is a link analysis algorithm that rates Web pages,
 *developed by Jon Kleinberg.
 *
 * The algorithm performs a series of iterations, each consisting of two basic steps:
 *
 * Authority Update: Update each node's Authority score to be equal to the sum of the Hub Scores of each node that
 * points to it.
 * That is, a node is given a high authority score by being linked from pages that are recognized as Hubs for information.
 * Hub Update: Update each node's Hub Score to be equal to the sum of the Authority Scores of each node that it
 * points to.
 * That is, a node is given a high hub score by linking to nodes that are considered to be authorities on the subject.
 *
 * The Hub score and Authority score for a node is calculated with the following algorithm:
 *  *Start with each node having a hub score and authority score of 1.
 *  *Run the Authority Update Rule
 *  *Run the Hub Update Rule
 *  *Normalize the values by dividing each Hub score by square root of the sum of the squares of all Hub scores, and
 *   dividing each Authority score by square root of the sum of the squares of all Authority scores.
 *  *Repeat from the second step as necessary.
 *
 * http://en.wikipedia.org/wiki/HITS_algorithm
 *
 */

public class HITS <K> implements GraphAlgorithm<K, Double, Double> {

    private Graph<K, Double, Double>AuthorityGraph=null;
    private Hits HubAuthority;
    private int maxIterations;

    public HITS(Hits choice, int maxIter){
        this.HubAuthority=choice;
        this.maxIterations=maxIter;
    }

    /**
     * this method will get a graph and process for Hub and Authority and it will return a graph( Hub or Authority
     * values).
     * @param HubGraph
     * @return Graph
     * @throws Exception
     */

    @Override
    public Graph<K, Double, Double> run(Graph<K, Double, Double> HubGraph) throws Exception {

        AuthorityGraph = (HubGraph.runVertexCentricIteration(new VertexHitsUpdater(), new HitsMessenger(), 2));

        for(int i=1; i<=maxIterations; i++) {

            HubGraph = (AuthorityGraph.reverse()).runVertexCentricIteration(new VertexHitsUpdater(), new HitsMessenger(), 2);

            if(!(i==maxIterations)) {
                AuthorityGraph = (HubGraph.reverse()).runVertexCentricIteration(new VertexHitsUpdater(), new HitsMessenger(), 2);
            }
        }

        if(HubAuthority.equals(Hits.HUB))
        {   return HubGraph;    }
        else
        {    return AuthorityGraph;   }

    }
    /**
     * Function that updates in odd superStep Iteration either the Hub or Authority values of a vertex by summing up
     * the partial Hits or Authority values from all incoming messages and then applying the normalization process on
     * even superstep for either Hub or Authority values.
     */
    @SuppressWarnings("serial")
    public static final class VertexHitsUpdater<K> extends VertexUpdateFunction<K, Double, Double> {

        static double norml;

        @Override
        public void updateVertex(Vertex<K, Double> vertex, MessageIterator<Double> inMessages) {

            double num=0.0;

            if (getSuperstepNumber() % 2 == 1) {

                for(double m:inMessages)
                    num+=m;
                norml+=Math.pow(num,2);
                setNewVertexValue(num);

            } else {

                double tmp=0.0;
                tmp=Math.sqrt(norml);
                setNewVertexValue(vertex.f1/tmp);

            }
        }
    }

    /**
     *Distributes in odd superstep iteration either the Hub or Authority values of a vertex among all target neighbor
     * vertices, and in even superstep iteration the normalization value to the target vertices.
     */
    @SuppressWarnings("serial")
    public static final class HitsMessenger<K> extends MessagingFunction<K, Double, Double, Double> {

        @Override
        public void sendMessages(Vertex<K, Double> vertex) {

            if(getSuperstepNumber()%2==1)

                sendMessageToAllNeighbors(vertex.f1);

            else {

                sendMessageTo(vertex.f0,vertex.f1);
            }
        }
    }
}

