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

package org.apache.flink.graph.bipartite.validation;

import org.apache.flink.graph.bipartite.BipartiteGraph;

import java.io.Serializable;

/**
 * A utility for defining validation criteria for different types of Bipartite Graphs.
 *
 * @param <KT> the key type of the top vertices
 * @param <KB> the key type of the bottom vertices
 * @param <VVT> the top vertices value type
 * @param <VVB> the bottom vertices value type
 * @param <EV> the edge value type
 */
public abstract class BipartiteGraphValidator<KT, KB, VVT, VVB, EV> implements Serializable {

	/**
	 * Check whether a graph is valid.
	 *
	 * @return a boolean stating whether a graph is valid
	 */
	public abstract boolean validate(BipartiteGraph<KT, KB, VVT, VVB, EV> bipartiteGraph) throws Exception;
}
