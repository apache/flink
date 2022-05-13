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

package org.apache.flink.graph.asm.result;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * A {@link GraphAlgorithm} result for a pair vertices.
 *
 * @param <K> graph ID type
 */
public interface BinaryResult<K> extends Serializable {

    /**
     * Get the first vertex ID.
     *
     * @return first vertex ID
     */
    K getVertexId0();

    /**
     * Set the first vertex ID.
     *
     * @param vertexId0 new vertex ID
     */
    void setVertexId0(K vertexId0);

    /**
     * Get the second vertex ID.
     *
     * @return second vertex ID
     */
    K getVertexId1();

    /**
     * Set the second vertex ID.
     *
     * @param vertexId1 new vertex ID
     */
    void setVertexId1(K vertexId1);

    /**
     * Output each input and a second result with the vertex order flipped.
     *
     * @param <T> graph ID type
     * @param <RT> result type
     */
    class MirrorResult<T, RT extends BinaryResult<T>> implements FlatMapFunction<RT, RT> {
        @Override
        public void flatMap(RT value, Collector<RT> out) throws Exception {
            out.collect(value);

            T tmp = value.getVertexId0();
            value.setVertexId0(value.getVertexId1());
            value.setVertexId1(tmp);

            out.collect(value);
        }
    }
}
