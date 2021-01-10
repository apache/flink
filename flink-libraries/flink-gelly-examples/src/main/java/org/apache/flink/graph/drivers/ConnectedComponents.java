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

package org.apache.flink.graph.drivers;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.GSAConnectedComponents;

/**
 * Driver for {@link org.apache.flink.graph.library.GSAConnectedComponents}.
 *
 * <p>The gather-sum-apply implementation is used because scatter-gather does not handle object
 * reuse (see FLINK-5891).
 */
public class ConnectedComponents<K extends Comparable<K>, VV, EV> extends DriverBase<K, VV, EV> {

    @Override
    public String getShortDescription() {
        return "ConnectedComponents";
    }

    @Override
    public String getLongDescription() {
        return "ConnectedComponents";
    }

    @Override
    public DataSet plan(Graph<K, VV, EV> graph) throws Exception {
        return graph.mapVertices(new MapVertices<>())
                .run(new GSAConnectedComponents<>(Integer.MAX_VALUE));
    }

    /**
     * Initialize vertices into separate components by setting each vertex value to the vertex ID.
     *
     * @param <T> vertex ID type
     * @param <VT> vertex value type
     */
    private static final class MapVertices<T, VT> implements MapFunction<Vertex<T, VT>, T> {
        @Override
        public T map(Vertex<T, VT> value) throws Exception {
            return value.f0;
        }
    }
}
