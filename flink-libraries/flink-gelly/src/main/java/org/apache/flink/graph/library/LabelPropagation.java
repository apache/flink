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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.graph.utils.GraphUtils.MapTo;
import org.apache.flink.types.NullValue;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * An implementation of the label propagation algorithm. The iterative algorithm detects communities
 * by propagating labels. In each iteration, a vertex adopts the label that is most frequent among
 * its neighbors' labels.
 *
 * <p>The initial vertex values are used as initial labels and are expected to be {@link
 * Comparable}. In case of a tie (i.e. two or more labels appear with the same frequency), the
 * algorithm picks the greater label. The algorithm converges when no vertex changes its value or
 * the maximum number of iterations has been reached. Note that different initializations might lead
 * to different results.
 *
 * @param <K> vertex identifier type
 * @param <VV> vertex value type which is used for comparison
 * @param <EV> edge value type
 */
@SuppressWarnings("serial")
public class LabelPropagation<K, VV extends Comparable<VV>, EV>
        implements GraphAlgorithm<K, VV, EV, DataSet<Vertex<K, VV>>> {

    private final int maxIterations;

    /**
     * Creates a new Label Propagation algorithm instance. The algorithm converges when vertices no
     * longer update their value or when the maximum number of iterations is reached.
     *
     * @see <a href="http://journals.aps.org/pre/abstract/10.1103/PhysRevE.76.036106">Near linear
     *     time algorithm to detect community structures in large-scale networks</a>
     * @param maxIterations The maximum number of iterations to run.
     */
    public LabelPropagation(int maxIterations) {
        this.maxIterations = maxIterations;
    }

    @Override
    public DataSet<Vertex<K, VV>> run(Graph<K, VV, EV> input) {

        TypeInformation<VV> valueType =
                ((TupleTypeInfo<?>) input.getVertices().getType()).getTypeAt(1);
        // iteratively adopt the most frequent label among the neighbors of each vertex
        return input.mapEdges(new MapTo<>(NullValue.getInstance()))
                .runScatterGatherIteration(
                        new SendNewLabelToNeighbors<>(valueType),
                        new UpdateVertexLabel<>(),
                        maxIterations)
                .getVertices();
    }

    /** Sends the vertex label to all out-neighbors. */
    public static final class SendNewLabelToNeighbors<K, VV extends Comparable<VV>>
            extends ScatterFunction<K, VV, VV, NullValue> implements ResultTypeQueryable<VV> {

        private final TypeInformation<VV> typeInformation;

        public SendNewLabelToNeighbors(TypeInformation<VV> typeInformation) {
            this.typeInformation = typeInformation;
        }

        public void sendMessages(Vertex<K, VV> vertex) {
            sendMessageToAllNeighbors(vertex.getValue());
        }

        @Override
        public TypeInformation<VV> getProducedType() {
            return typeInformation;
        }
    }

    /**
     * Function that updates the value of a vertex by adopting the most frequent label among its
     * in-neighbors.
     */
    public static final class UpdateVertexLabel<K, VV extends Comparable<VV>>
            extends GatherFunction<K, VV, VV> {

        public void updateVertex(Vertex<K, VV> vertex, MessageIterator<VV> inMessages) {
            Map<VV, Long> labelsWithFrequencies = new HashMap<>();

            long maxFrequency = 1;
            VV mostFrequentLabel = vertex.getValue();

            // store the labels with their frequencies
            for (VV msg : inMessages) {
                if (labelsWithFrequencies.containsKey(msg)) {
                    long currentFreq = labelsWithFrequencies.get(msg);
                    labelsWithFrequencies.put(msg, currentFreq + 1);
                } else {
                    labelsWithFrequencies.put(msg, 1L);
                }
            }
            // select the most frequent label: if two or more labels have the
            // same frequency, the node adopts the label with the highest value
            for (Entry<VV, Long> entry : labelsWithFrequencies.entrySet()) {
                if (entry.getValue() == maxFrequency) {
                    // check the label value to break ties
                    if (entry.getKey().compareTo(mostFrequentLabel) > 0) {
                        mostFrequentLabel = entry.getKey();
                    }
                } else if (entry.getValue() > maxFrequency) {
                    maxFrequency = entry.getValue();
                    mostFrequentLabel = entry.getKey();
                }
            }
            setNewVertexValue(mostFrequentLabel);
        }
    }
}
