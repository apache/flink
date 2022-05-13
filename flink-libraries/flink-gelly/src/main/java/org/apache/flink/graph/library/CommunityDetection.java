/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.graph.library;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;

import java.util.Map;
import java.util.TreeMap;

/**
 * Community Detection Algorithm.
 *
 * <p>The Vertex values of the input Graph provide the initial label assignments.
 *
 * <p>Initially, each vertex is assigned a tuple formed of its own initial value along with a score
 * equal to 1.0. The vertices propagate their labels and max scores in iterations, each time
 * adopting the label with the highest score from the list of received messages. The chosen label is
 * afterwards re-scored using the fraction delta/the superstep number. Delta is passed as a
 * parameter and has 0.5 as a default value.
 *
 * @param <K> the Vertex ID type
 */
public class CommunityDetection<K>
        implements GraphAlgorithm<K, Long, Double, Graph<K, Long, Double>> {

    private int maxIterations;

    private double delta;

    /**
     * Creates a new Community Detection algorithm instance. The algorithm converges when vertices
     * no longer update their value or when the maximum number of iterations is reached.
     *
     * @see <a href="http://arxiv.org/pdf/0808.2633.pdf">Towards real-time community detection in
     *     large networks</a>
     * @param maxIterations The maximum number of iterations to run.
     * @param delta The hop attenuation parameter. Its default value is 0.5.
     */
    public CommunityDetection(int maxIterations, double delta) {

        this.maxIterations = maxIterations;
        this.delta = delta;
    }

    @Override
    public Graph<K, Long, Double> run(Graph<K, Long, Double> graph) {

        DataSet<Vertex<K, Tuple2<Long, Double>>> initializedVertices =
                graph.getVertices().map(new AddScoreToVertexValuesMapper<>());

        Graph<K, Tuple2<Long, Double>, Double> graphWithScoredVertices =
                Graph.fromDataSet(initializedVertices, graph.getEdges(), graph.getContext())
                        .getUndirected();

        return graphWithScoredVertices
                .runScatterGatherIteration(
                        new LabelMessenger<>(), new VertexLabelUpdater<>(delta), maxIterations)
                .mapVertices(new RemoveScoreFromVertexValuesMapper<>());
    }

    @SuppressWarnings("serial")
    private static final class LabelMessenger<K>
            extends ScatterFunction<K, Tuple2<Long, Double>, Tuple2<Long, Double>, Double> {

        @Override
        public void sendMessages(Vertex<K, Tuple2<Long, Double>> vertex) throws Exception {

            for (Edge<K, Double> edge : getEdges()) {
                sendMessageTo(
                        edge.getTarget(),
                        new Tuple2<>(vertex.getValue().f0, vertex.getValue().f1 * edge.getValue()));
            }
        }
    }

    @SuppressWarnings("serial")
    private static final class VertexLabelUpdater<K>
            extends GatherFunction<K, Tuple2<Long, Double>, Tuple2<Long, Double>> {

        private double delta;

        public VertexLabelUpdater(double delta) {
            this.delta = delta;
        }

        @Override
        public void updateVertex(
                Vertex<K, Tuple2<Long, Double>> vertex,
                MessageIterator<Tuple2<Long, Double>> inMessages)
                throws Exception {

            // we would like these two maps to be ordered
            Map<Long, Double> receivedLabelsWithScores = new TreeMap<>();
            Map<Long, Double> labelsWithHighestScore = new TreeMap<>();

            for (Tuple2<Long, Double> message : inMessages) {
                // split the message into received label and score
                long receivedLabel = message.f0;
                double receivedScore = message.f1;

                // if the label was received before
                if (receivedLabelsWithScores.containsKey(receivedLabel)) {
                    double newScore = receivedScore + receivedLabelsWithScores.get(receivedLabel);
                    receivedLabelsWithScores.put(receivedLabel, newScore);
                } else {
                    // first time we see the label
                    receivedLabelsWithScores.put(receivedLabel, receivedScore);
                }

                // store the labels with the highest scores
                if (labelsWithHighestScore.containsKey(receivedLabel)) {
                    double currentScore = labelsWithHighestScore.get(receivedLabel);
                    if (currentScore < receivedScore) {
                        // record the highest score
                        labelsWithHighestScore.put(receivedLabel, receivedScore);
                    }
                } else {
                    // first time we see this label
                    labelsWithHighestScore.put(receivedLabel, receivedScore);
                }
            }

            if (receivedLabelsWithScores.size() > 0) {
                // find the label with the highest score from the ones received
                double maxScore = -Double.MAX_VALUE;
                long maxScoreLabel = vertex.getValue().f0;
                for (Map.Entry<Long, Double> entry : receivedLabelsWithScores.entrySet()) {
                    long curLabel = entry.getKey();
                    double curValue = entry.getValue();

                    if (curValue > maxScore) {
                        maxScore = curValue;
                        maxScoreLabel = curLabel;
                    }
                }

                // find the highest score of maxScoreLabel
                double highestScore = labelsWithHighestScore.get(maxScoreLabel);
                // re-score the new label
                if (maxScoreLabel != vertex.getValue().f0) {
                    highestScore -= delta / getSuperstepNumber();
                }
                // else delta = 0
                // update own label
                setNewVertexValue(new Tuple2<>(maxScoreLabel, highestScore));
            }
        }
    }

    @SuppressWarnings("serial")
    @ForwardedFields("f0")
    private static final class AddScoreToVertexValuesMapper<K>
            implements MapFunction<Vertex<K, Long>, Vertex<K, Tuple2<Long, Double>>> {

        public Vertex<K, Tuple2<Long, Double>> map(Vertex<K, Long> vertex) {
            return new Vertex<>(vertex.getId(), new Tuple2<>(vertex.getValue(), 1.0));
        }
    }

    @SuppressWarnings("serial")
    private static final class RemoveScoreFromVertexValuesMapper<K>
            implements MapFunction<Vertex<K, Tuple2<Long, Double>>, Long> {

        @Override
        public Long map(Vertex<K, Tuple2<Long, Double>> vertex) throws Exception {
            return vertex.getValue().f0;
        }
    }
}
