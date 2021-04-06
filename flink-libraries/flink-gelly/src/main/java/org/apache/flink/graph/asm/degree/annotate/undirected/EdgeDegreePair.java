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

package org.apache.flink.graph.asm.degree.annotate.undirected;

import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.DegreeAnnotationFunctions.JoinEdgeDegreeWithVertexDegree;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingBase;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingDataSet;
import org.apache.flink.graph.utils.proxy.OptionalBoolean;
import org.apache.flink.types.LongValue;

/**
 * Annotates edges of an undirected graph with the degree of both the source and target degree
 * vertices.
 *
 * @param <K> ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class EdgeDegreePair<K, VV, EV>
        extends GraphAlgorithmWrappingDataSet<
                K, VV, EV, Edge<K, Tuple3<EV, LongValue, LongValue>>> {

    // Optional configuration
    private OptionalBoolean reduceOnTargetId = new OptionalBoolean(false, false);

    /**
     * The degree can be counted from either the edge source or target IDs. By default the source
     * IDs are counted. Reducing on target IDs may optimize the algorithm if the input edge list is
     * sorted by target ID.
     *
     * @param reduceOnTargetId set to {@code true} if the input edge list is sorted by target ID
     * @return this
     */
    public EdgeDegreePair<K, VV, EV> setReduceOnTargetId(boolean reduceOnTargetId) {
        this.reduceOnTargetId.set(reduceOnTargetId);

        return this;
    }

    @Override
    protected void mergeConfiguration(GraphAlgorithmWrappingBase other) {
        super.mergeConfiguration(other);

        EdgeDegreePair rhs = (EdgeDegreePair) other;

        reduceOnTargetId.mergeWith(rhs.reduceOnTargetId);
    }

    @Override
    public DataSet<Edge<K, Tuple3<EV, LongValue, LongValue>>> runInternal(Graph<K, VV, EV> input)
            throws Exception {
        // s, t, d(s)
        DataSet<Edge<K, Tuple2<EV, LongValue>>> edgeSourceDegrees =
                input.run(
                        new EdgeSourceDegree<K, VV, EV>()
                                .setReduceOnTargetId(reduceOnTargetId.get())
                                .setParallelism(parallelism));

        // t, d(t)
        DataSet<Vertex<K, LongValue>> vertexDegrees =
                input.run(
                        new VertexDegree<K, VV, EV>()
                                .setReduceOnTargetId(reduceOnTargetId.get())
                                .setParallelism(parallelism));

        // s, t, (d(s), d(t))
        return edgeSourceDegrees
                .join(vertexDegrees, JoinHint.REPARTITION_HASH_SECOND)
                .where(1)
                .equalTo(0)
                .with(new JoinEdgeDegreeWithVertexDegree<>())
                .setParallelism(parallelism)
                .name("Edge target degree");
    }
}
