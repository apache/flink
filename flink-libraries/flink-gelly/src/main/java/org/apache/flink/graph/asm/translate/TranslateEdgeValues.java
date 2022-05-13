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

package org.apache.flink.graph.asm.translate;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingBase;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingGraph;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.graph.asm.translate.Translate.translateEdgeValues;

/**
 * Translate {@link Edge} values using the given {@link TranslateFunction}.
 *
 * @param <K> vertex ID type
 * @param <VV> vertex value type
 * @param <OLD> old edge value type
 * @param <NEW> new edge value type
 */
public class TranslateEdgeValues<K, VV, OLD, NEW>
        extends GraphAlgorithmWrappingGraph<K, VV, OLD, K, VV, NEW> {

    // Required configuration
    private TranslateFunction<OLD, NEW> translator;

    /**
     * Translate {@link Edge} values using the given {@link TranslateFunction}.
     *
     * @param translator implements conversion from {@code OLD} to {@code NEW}
     */
    public TranslateEdgeValues(TranslateFunction<OLD, NEW> translator) {
        Preconditions.checkNotNull(translator);

        this.translator = translator;
    }

    @Override
    protected boolean canMergeConfigurationWith(GraphAlgorithmWrappingBase other) {
        if (!super.canMergeConfigurationWith(other)) {
            return false;
        }

        TranslateEdgeValues rhs = (TranslateEdgeValues) other;

        return translator == rhs.translator;
    }

    @Override
    public Graph<K, VV, NEW> runInternal(Graph<K, VV, OLD> input) throws Exception {
        DataSet<Edge<K, NEW>> translatedEdges =
                translateEdgeValues(input.getEdges(), translator, parallelism);

        return Graph.fromDataSet(input.getVertices(), translatedEdges, input.getContext());
    }
}
