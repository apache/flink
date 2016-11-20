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

package org.apache.flink.graph.validation;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.bipartite.BipartiteEdge;
import org.apache.flink.graph.bipartite.BipartiteGraph;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class InvalidBipartiteVertexIdsValidatorTest {

	private ExecutionEnvironment executionEnvironment = ExecutionEnvironment.createCollectionsEnvironment();

	private DataSet<Vertex<Integer, String>> topVertices = executionEnvironment.fromCollection(Arrays.asList(
		new Vertex<>(1, "1"),
		new Vertex<>(2, "2"),
		new Vertex<>(3, "3")
	));


	private DataSet<Vertex<Integer, String>> bottomVertices = executionEnvironment.fromCollection(Arrays.asList(
		new Vertex<>(4, "1"),
		new Vertex<>(5, "5"),
		new Vertex<>(6, "6")
	));

	private InvalidBipartiteVertexIdsValidator<Integer, Integer, String, String, String> validator
		= new InvalidBipartiteVertexIdsValidator<>();

	@Test
	public void testGraphIsInvalidIfTopIdsAreInvalid() throws Exception {
		DataSet<BipartiteEdge<Integer, Integer, String>> edges = executionEnvironment.fromCollection(Arrays.asList(
			new BipartiteEdge<>(100, 4, "")
		));

		BipartiteGraph<Integer, Integer, String, String, String> graph
			= BipartiteGraph.fromDataSet(topVertices, bottomVertices, edges, executionEnvironment);

		assertFalse(validator.validate(graph));
	}

	@Test
	public void testGraphIsInvalidIfBottomIdsAreInvalid() throws Exception {
		DataSet<BipartiteEdge<Integer, Integer, String>> edges = executionEnvironment.fromCollection(Arrays.asList(
			new BipartiteEdge<>(3, 100, "")
		));

		BipartiteGraph<Integer, Integer, String, String, String> graph
			= BipartiteGraph.fromDataSet(topVertices, bottomVertices, edges, executionEnvironment);

		assertFalse(validator.validate(graph));
	}

	@Test
	public void testGraphIsValidIfAllIdsAreValid() throws Exception {
		DataSet<BipartiteEdge<Integer, Integer, String>> edges = executionEnvironment.fromCollection(Arrays.asList(
			new BipartiteEdge<>(3, 4, "")
		));

		BipartiteGraph<Integer, Integer, String, String, String> graph
			= BipartiteGraph.fromDataSet(topVertices, bottomVertices, edges, executionEnvironment);

		assertTrue(validator.validate(graph));
	}

}
