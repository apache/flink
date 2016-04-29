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
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.StringValue;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.apache.flink.graph.asm.translate.Translate.translateVertexValues;
import static org.junit.Assert.assertEquals;

public class TranslateTest {

	private Graph<LongValue, LongValue, LongValue> graph;

	private String expectedVertexResult =
		"(0,0)\n" +
		"(1,1)\n" +
		"(2,2)\n" +
		"(3,3)\n" +
		"(4,4)\n" +
		"(5,5)\n" +
		"(6,6)\n" +
		"(7,7)\n" +
		"(8,8)\n" +
		"(9,9)";

	private String expectedEdgeResult =
		"(0,0,0)\n" +
		"(1,1,1)\n" +
		"(2,2,2)\n" +
		"(3,3,3)\n" +
		"(4,4,4)\n" +
		"(5,5,5)\n" +
		"(6,6,6)\n" +
		"(7,7,7)\n" +
		"(8,8,8)\n" +
		"(9,9,9)";

	@Before
	public void setup() {
		ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();

		int count = 10;

		List<Vertex<LongValue, LongValue>> vertexList = new LinkedList<>();
		List<Edge<LongValue, LongValue>> edgeList = new LinkedList<>();

		for (long l = 0 ; l < count ; l++) {
			LongValue lv = new LongValue(l);
			vertexList.add(new Vertex<>(lv, lv));
			edgeList.add(new Edge<>(lv, lv, lv));
		}

		graph = Graph.fromCollection(vertexList, edgeList, env);
	}

	@Test
	public void testTranslateGraphIds()
			throws Exception {
		Graph<StringValue,LongValue, LongValue> stringIdGraph = graph
			.translateGraphIds(new LongValueToStringValue());

		for (Vertex<StringValue, LongValue> vertex : stringIdGraph.getVertices().collect()) {
			assertEquals(StringValue.class, vertex.f0.getClass());
			assertEquals(LongValue.class, vertex.f1.getClass());
		}

		for (Edge<StringValue, LongValue> edge : stringIdGraph.getEdges().collect()) {
			assertEquals(StringValue.class, edge.f0.getClass());
			assertEquals(StringValue.class, edge.f1.getClass());
			assertEquals(LongValue.class, edge.f2.getClass());
		}

		TestBaseUtils.compareResultAsText(stringIdGraph.getVertices().collect(), expectedVertexResult);
		TestBaseUtils.compareResultAsText(stringIdGraph.getEdges().collect(), expectedEdgeResult);
	}

	@Test
	public void testTranslateVertexValues()
			throws Exception {
		DataSet<Vertex<LongValue, StringValue>> vertexSet = graph
			.translateVertexValues(new LongValueToStringValue())
			.getVertices();

		for (Vertex<LongValue, StringValue> vertex : vertexSet.collect()) {
			assertEquals(LongValue.class, vertex.f0.getClass());
			assertEquals(StringValue.class, vertex.f1.getClass());
		}

		TestBaseUtils.compareResultAsText(vertexSet.collect(), expectedVertexResult);
	}

	@Test
	public void testTranslateEdgeValues()
			throws Exception {
		DataSet<Edge<LongValue, StringValue>> edgeSet = graph
			.translateEdgeValues(new LongValueToStringValue())
			.getEdges();

		for (Edge<LongValue, StringValue> edge : edgeSet.collect()) {
			assertEquals(LongValue.class, edge.f0.getClass());
			assertEquals(LongValue.class, edge.f1.getClass());
			assertEquals(StringValue.class, edge.f2.getClass());
		}

		TestBaseUtils.compareResultAsText(edgeSet.collect(), expectedEdgeResult);
	}
}
