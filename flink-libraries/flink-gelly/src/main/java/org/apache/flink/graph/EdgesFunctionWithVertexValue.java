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

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * Interface to be implemented by the function applied to a vertex neighborhood
 * in the {@link Graph#groupReduceOnEdges(EdgesFunctionWithVertexValue, EdgeDirection)}
 * method.
 *
 * @param <K> the vertex key type
 * @param <VV> the vertex value type
 * @param <EV> the edge value type
 * @param <O> the type of the return value
 */
public interface EdgesFunctionWithVertexValue<K, VV, EV, O> extends Function, Serializable {

	/**
	 * This method is called per vertex and can iterate over all of its neighboring edges
	 * with the specified direction.
	 *
	 * <p>If called with {@link EdgeDirection#OUT} the group will contain
	 * the out-edges of the grouping vertex.
	 * If called with {@link EdgeDirection#IN} the group will contain
	 * the in-edges of the grouping vertex.
	 * If called with {@link EdgeDirection#ALL} the group will contain
	 * all edges of the grouping vertex.
	 *
	 * <p>The method can emit any number of output elements, including none.
	 *
	 * @param vertex the grouping vertex
	 * @param edges the neighboring edges of the grouping vertex.
	 * @param out the collector to emit results to
	 * @throws Exception
	*/
	void iterateEdges(Vertex<K, VV> vertex, Iterable<Edge<K, EV>> edges, Collector<O> out) throws Exception;
}
