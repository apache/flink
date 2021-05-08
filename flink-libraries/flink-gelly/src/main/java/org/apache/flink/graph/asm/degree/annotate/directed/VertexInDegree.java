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

package org.apache.flink.graph.asm.degree.annotate.directed;

import org.apache.flink.api.common.operators.base.ReduceOperatorBase.CombineHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.DegreeAnnotationFunctions.DegreeCount;
import org.apache.flink.graph.asm.degree.annotate.DegreeAnnotationFunctions.JoinVertexWithVertexDegree;
import org.apache.flink.graph.asm.degree.annotate.DegreeAnnotationFunctions.MapEdgeToTargetId;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingBase;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingDataSet;
import org.apache.flink.graph.utils.proxy.OptionalBoolean;
import org.apache.flink.types.LongValue;

/**
 * Annotates vertices of a directed graph with the in-degree.
 *
 * @param <K> graph label type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class VertexInDegree<K, VV, EV>
        extends GraphAlgorithmWrappingDataSet<K, VV, EV, Vertex<K, LongValue>> {

    // Optional configuration
    private OptionalBoolean includeZeroDegreeVertices = new OptionalBoolean(false, true);

    /**
     * By default only the edge set is processed for the computation of degree. When this flag is
     * set an additional join is performed against the vertex set in order to output vertices with
     * an in-degree of zero.
     *
     * @param includeZeroDegreeVertices whether to output vertices with an in-degree of zero
     * @return this
     */
    public VertexInDegree<K, VV, EV> setIncludeZeroDegreeVertices(
            boolean includeZeroDegreeVertices) {
        this.includeZeroDegreeVertices.set(includeZeroDegreeVertices);

        return this;
    }

    @Override
    protected boolean canMergeConfigurationWith(GraphAlgorithmWrappingBase other) {
        if (!super.canMergeConfigurationWith(other)) {
            return false;
        }

        VertexInDegree rhs = (VertexInDegree) other;

        return !includeZeroDegreeVertices.conflictsWith(rhs.includeZeroDegreeVertices);
    }

    @Override
    protected void mergeConfiguration(GraphAlgorithmWrappingBase other) {
        super.mergeConfiguration(other);

        VertexInDegree rhs = (VertexInDegree) other;

        includeZeroDegreeVertices.mergeWith(rhs.includeZeroDegreeVertices);
    }

    @Override
    public DataSet<Vertex<K, LongValue>> runInternal(Graph<K, VV, EV> input) throws Exception {
        // t
        DataSet<Vertex<K, LongValue>> targetIds =
                input.getEdges()
                        .map(new MapEdgeToTargetId<>())
                        .setParallelism(parallelism)
                        .name("Edge to target ID");

        // t, d(t)
        DataSet<Vertex<K, LongValue>> targetDegree =
                targetIds
                        .groupBy(0)
                        .reduce(new DegreeCount<>())
                        .setCombineHint(CombineHint.HASH)
                        .setParallelism(parallelism)
                        .name("Degree count");

        if (includeZeroDegreeVertices.get()) {
            targetDegree =
                    input.getVertices()
                            .leftOuterJoin(targetDegree)
                            .where(0)
                            .equalTo(0)
                            .with(new JoinVertexWithVertexDegree<>())
                            .setParallelism(parallelism)
                            .name("Zero degree vertices");
        }

        return targetDegree;
    }
}
