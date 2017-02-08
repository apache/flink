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

package org.apache.flink.graph.library;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.test.util.TestBaseUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.util.ArrayList;
import java.util.List;

/**
 * Test for {@link BoruvkaMST}
 */
@RunWith(Parameterized.class)
public class BoruvkaMSTITCase extends MultipleProgramsTestBase {

	private List<Edge<Long, Double>> longDoubleEdges;
	private List<Edge<String, Integer>> strIntEdges;

	private static final String STR_INT_RESULT = "V1,V3,1\n" + "V2,V3,5\n" + "V2,V5,3\n" + "V3,V6,4\n" + "V4,V6,2";
	private static final String LONG_DOUBLE_RESULT = "1,3,6.0\n" + "2,3,7.0\n" + "2,4,1.0\n"
			+ "4,6,3.0\n" + "5,6,2.0\n" + "5,10,18.0\n" + "7,8,15.0\n" + "7,9,5.0\n" + "7,11,10.0\n" + "10,12,4.0\n"
			+ "11,12,12.0";

	public BoruvkaMSTITCase(TestExecutionMode mode) {
		super(mode);
	}

	@Before
	public void initEdgeSet() {
		longDoubleEdges = new ArrayList<>();
		longDoubleEdges.add(new Edge<>(1L, 2L, 13.0));
		longDoubleEdges.add(new Edge<>(1L, 3L, 6.0));
		longDoubleEdges.add(new Edge<>(2L, 3L, 7.0));
		longDoubleEdges.add(new Edge<>(2L, 4L, 1.0));
		longDoubleEdges.add(new Edge<>(3L, 4L, 14.0));
		longDoubleEdges.add(new Edge<>(3L, 5L, 8.0));
		longDoubleEdges.add(new Edge<>(3L, 8L, 20.0));
		longDoubleEdges.add(new Edge<>(4L, 5L, 9.0));
		longDoubleEdges.add(new Edge<>(4L, 6L, 3.0));
		longDoubleEdges.add(new Edge<>(5L, 6L, 2.0));
		longDoubleEdges.add(new Edge<>(5L, 10L, 18.0));
		longDoubleEdges.add(new Edge<>(7L, 8L, 15.0));
		longDoubleEdges.add(new Edge<>(7L, 9L, 5.0));
		longDoubleEdges.add(new Edge<>(7L, 10L, 19.0));
		longDoubleEdges.add(new Edge<>(7L, 11L, 10.0));
		longDoubleEdges.add(new Edge<>(8L, 10L, 17.0));
		longDoubleEdges.add(new Edge<>(9L, 11L, 11.0));
		longDoubleEdges.add(new Edge<>(10L, 11L, 16.0));
		longDoubleEdges.add(new Edge<>(10L, 12L, 4.0));
		longDoubleEdges.add(new Edge<>(11L, 12L, 12.0));

		strIntEdges = new ArrayList<>();
		strIntEdges.add(new Edge<>("V1", "V2", 6));
		strIntEdges.add(new Edge<>("V1", "V3", 1));
		strIntEdges.add(new Edge<>("V1", "V4", 5));
		strIntEdges.add(new Edge<>("V2", "V3", 5));
		strIntEdges.add(new Edge<>("V2", "V5", 3));
		strIntEdges.add(new Edge<>("V3", "V4", 5));
		strIntEdges.add(new Edge<>("V3", "V5", 6));
		strIntEdges.add(new Edge<>("V3", "V6", 4));
		strIntEdges.add(new Edge<>("V4", "V6", 2));
		strIntEdges.add(new Edge<>("V5", "V6", 6));
	}

	@Test
	public void testWithLongDoubleGraph() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Double> graph = Graph.fromDataSet(env.fromCollection(longDoubleEdges),
				new InitLong(), env);
		BoruvkaMST<Long, Long, Double> boruvkaMST = new BoruvkaMST<>(100, 2);
		boruvkaMST.setParallelism(4);
		DataSet<String> result = boruvkaMST.run(graph).map(new ExtractEdgeStr<Long, Double>());
		TestBaseUtils.compareResultAsText(result.collect(), LONG_DOUBLE_RESULT);
	}

	@Test
	public void testWithStrIntGraph() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<String, String, Integer> graph = Graph.fromDataSet(env.fromCollection(strIntEdges),
				new InitStr(), env);
		BoruvkaMST<String, String, Integer> boruvkaMST = new BoruvkaMST<>(1);
		boruvkaMST.setMaxIterationTimes(100);
		DataSet<String> result = boruvkaMST.run(graph).map(new ExtractEdgeStr<String, Integer>());
		TestBaseUtils.compareResultAsText(result.collect(), STR_INT_RESULT);
	}

	@SuppressWarnings("serial")
	private static final class InitLong implements MapFunction<Long, Long> {
		public Long map(Long id) {
			return id;
		}
	}

	@SuppressWarnings("serial")
	private static final class InitStr implements MapFunction<String, String> {
		public String map(String id) {
			return id;
		}
	}

	@SuppressWarnings("serial")
	private static final class ExtractEdgeStr<K, W> implements MapFunction<Edge<K, W>, String> {
		@Override
		public String map(Edge<K, W> value) throws Exception {
			return value.getSource() + "," + value.getTarget() + "," + value.getValue();
		}
	}

}
