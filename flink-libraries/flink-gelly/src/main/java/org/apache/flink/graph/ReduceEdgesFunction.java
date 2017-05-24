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

import java.io.Serializable;

/**
 * Interface to be implemented by the function applied to a vertex neighborhood
 * in the {@link Graph#reduceOnEdges(org.apache.flink.graph.ReduceEdgesFunction, EdgeDirection)} method.
 *
 * @param <EV> the edge value type
 */
public interface ReduceEdgesFunction<EV> extends Function, Serializable {

	/**
	 * It combines two neighboring edge values into one new value of the same type.
	 * For each vertex, this function is consecutively called,
	 * until only a single value for each edge remains.
	 *
	 * @param firstEdgeValue the first neighboring edge value to combine
	 * @param secondEdgeValue the second neighboring edge value to combine
	 * @return the combined value of both input values
	 */
	EV reduceEdges(EV firstEdgeValue, EV secondEdgeValue);
}
