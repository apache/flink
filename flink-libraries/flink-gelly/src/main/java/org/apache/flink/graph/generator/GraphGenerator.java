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

package org.apache.flink.graph.generator;

import org.apache.flink.graph.Graph;

/**
 * Graph generators shall be
 * - parallelizable, in order to create large datasets
 * - scale-free, generating the same graph regardless of parallelism
 * - thrifty, using as few operators as possible
 *
 * <p>Graph generators should prefer to emit edges sorted by the source label.
 *
 * @param <K> graph ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public interface GraphGenerator<K, VV, EV> {

	/**
	 * Generates the configured graph.
	 *
	 * @return generated graph
	 */
	Graph<K, VV, EV> generate();

	/**
	 * Override the operator parallelism.
	 *
	 * @param parallelism operator parallelism
	 * @return this
	 */
	GraphGenerator<K, VV, EV> setParallelism(int parallelism);
}
