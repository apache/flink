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
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.gsa.ApplyFunction;
import org.apache.flink.graph.gsa.GatherFunction;
import org.apache.flink.graph.gsa.Neighbor;
import org.apache.flink.graph.gsa.SumFunction;

/**
 * This is an implementation of the Single Source Shortest Paths algorithm, using a gather-sum-apply
 * iteration.
 */
public class GSASingleSourceShortestPaths<K, VV>
        implements GraphAlgorithm<K, VV, Double, DataSet<Vertex<K, Double>>> {

    private final K srcVertexId;
    private final Integer maxIterations;

    /**
     * Creates an instance of the GSA SingleSourceShortestPaths algorithm.
     *
     * @param srcVertexId The ID of the source vertex.
     * @param maxIterations The maximum number of iterations to run.
     */
    public GSASingleSourceShortestPaths(K srcVertexId, Integer maxIterations) {
        this.srcVertexId = srcVertexId;
        this.maxIterations = maxIterations;
    }

    @Override
    public DataSet<Vertex<K, Double>> run(Graph<K, VV, Double> input) {

        return input.mapVertices(new InitVerticesMapper<>(srcVertexId))
                .runGatherSumApplyIteration(
                        new CalculateDistances(),
                        new ChooseMinDistance(),
                        new UpdateDistance<>(),
                        maxIterations)
                .getVertices();
    }

    @SuppressWarnings("serial")
    private static final class InitVerticesMapper<K, VV>
            implements MapFunction<Vertex<K, VV>, Double> {

        private K srcVertexId;

        public InitVerticesMapper(K srcId) {
            this.srcVertexId = srcId;
        }

        public Double map(Vertex<K, VV> value) {
            if (value.f0.equals(srcVertexId)) {
                return 0.0;
            } else {
                return Double.MAX_VALUE;
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Single Source Shortest Path UDFs
    // --------------------------------------------------------------------------------------------

    @SuppressWarnings("serial")
    private static final class CalculateDistances extends GatherFunction<Double, Double, Double> {

        public Double gather(Neighbor<Double, Double> neighbor) {
            return neighbor.getNeighborValue() + neighbor.getEdgeValue();
        }
    }

    @SuppressWarnings("serial")
    private static final class ChooseMinDistance extends SumFunction<Double, Double, Double> {

        public Double sum(Double newValue, Double currentValue) {
            return Math.min(newValue, currentValue);
        }
    }

    @SuppressWarnings("serial")
    private static final class UpdateDistance<K> extends ApplyFunction<K, Double, Double> {

        public void apply(Double newDistance, Double oldDistance) {
            if (newDistance < oldDistance) {
                setResult(newDistance);
            }
        }
    }
}
