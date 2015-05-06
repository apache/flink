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

package org.apache.flink.graph;

import java.io.Serializable;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Interface to be implemented by the function applied to a vertex neighborhood
 * in the {@link Graph#groupReduceOnNeighbors(NeighborsFunctionWithVertexValue, EdgeDirection)}
 * method.
 *
 * @param <K> the vertex key type
 * @param <VV> the vertex value type
 * @param <EV> the edge value type
 * @param <O> the type of the return value
 */
public interface NeighborsFunctionWithVertexValue<K, VV, EV, O> extends Function, Serializable {

	void iterateNeighbors(Vertex<K, VV> vertex, Iterable<Tuple2<Edge<K, EV>, Vertex<K, VV>>> neighbors, Collector<O> out) throws Exception;
}
